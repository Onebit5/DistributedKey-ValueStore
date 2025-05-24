// Distributed Key-Value Store
// Components:
// 1. Node Management and Cluster Communication
// 2. Consistent Hashing & Data Distribution
// 3. Data Replication
// 4. Leader Election
// 5. Client API
// 6. Storage Engine

#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <chrono>
#include <algorithm>
#include <random>
#include <optional>
#include <condition_variable>
#include <atomic>
#include <cassert>
#include <asio.hpp>
#include <nlohmann/json.hpp>
#include <iomanip>

#if __cplusplus >= 201703L || _MSVC_LANG >= 201703L
#include <optional>
#include <unordered_set>
#else
// Fallback for older C++ standards
namespace std {
    // Define nullopt_t structure and nullopt constant
    struct nullopt_t {
        struct tag_t {};
        explicit constexpr nullopt_t(tag_t) {}
    };
    constexpr nullopt_t nullopt{ nullopt_t::tag_t{} };

    template<typename T>
    class optional {
    private:
        bool has_value_;
        T value_;
    public:
        optional() : has_value_(false) {}
        optional(const T& val) : has_value_(true), value_(val) {}
        optional(nullopt_t) : has_value_(false) {}

        bool has_value() const { return has_value_; }
        T value_or(const T& default_val) const { return has_value_ ? value_ : default_val; }
        T value() const { return value_; }
        operator bool() const { return has_value_; }
    };
}
#endif

void printTimestampedMessage(const std::string& message);

namespace dkv {

    using json = nlohmann::json;
    using namespace std::chrono_literals;

    // Forward declarations
    class Node;
    class ConsistentHash;
    class ReplicationManager;
    class LeaderElection;
    class StorageEngine;
    class ClusterManager;
    class ClientHandler;

    // Types
    using Key = std::string;
    using Value = std::string;
    using NodeId = std::string;
    using Hash = uint64_t;

    // ---------- 1. Node Management ----------

    // Represents a node in the cluster
    class Node {
    public:
        Node(NodeId id, std::string ip, int port)
            : id_(id), ip_(ip), port_(port), is_alive_(true), last_heartbeat_(std::chrono::steady_clock::now()) {
        }

        NodeId getId() const { return id_; }
        std::string getIp() const { return ip_; }
        int getPort() const { return port_; }
        bool isAlive() const { return is_alive_; }

        void markAlive() {
            is_alive_ = true;
            last_heartbeat_ = std::chrono::steady_clock::now();
        }

        void markDead() { is_alive_ = false; }

        std::chrono::steady_clock::time_point getLastHeartbeat() const {
            return last_heartbeat_;
        }

        void setStorageEngine(std::shared_ptr<StorageEngine> engine) {
            storage_engine_ = engine;
        }

        std::shared_ptr<StorageEngine> getStorageEngine() const {
            return storage_engine_.lock();
        }

    private:
        NodeId id_;
        std::string ip_;
        int port_;
        bool is_alive_;
        std::chrono::steady_clock::time_point last_heartbeat_;
        std::weak_ptr<StorageEngine> storage_engine_;
    };

    // Cluster membership and gossip protocol
    class ClusterManager {
    public:
        ClusterManager(std::shared_ptr<Node> local_node)
            : local_node_(local_node), running_(false) {
        }

        ~ClusterManager() {
            stop();
        }

        void start() {
            running_ = true;
            gossip_thread_ = std::thread(&ClusterManager::gossipLoop, this);
        }

        void stop() {
            running_ = false;
            if (gossip_thread_.joinable()) {
                gossip_thread_.join();
            }
        }

        void addNode(std::shared_ptr<Node> node) {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            nodes_[node->getId()] = node;
        }

        void removeNode(const NodeId& node_id) {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            nodes_.erase(node_id);
        }

        std::shared_ptr<Node> getNode(const NodeId& node_id) {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            auto it = nodes_.find(node_id);
            if (it != nodes_.end()) {
                return it->second;
            }
            return nullptr;
        }

        std::vector<std::shared_ptr<Node>> getAliveNodes() {
            std::vector<std::shared_ptr<Node>> alive_nodes;

            std::lock_guard<std::mutex> lock(nodes_mutex_);
            for (const auto& pair : nodes_) {
                const auto& node = pair.second;
                if (node->isAlive()) {
                    alive_nodes.push_back(node);
                }
            }

            return alive_nodes;
        }

        void handleGossipMessage(const NodeId& from_node, const json& gossip_data) {
            // Process received gossip data
            for (const auto& node_data : gossip_data["nodes"]) {
                NodeId node_id = node_data["id"];
                std::string ip = node_data["ip"];
                int port = node_data["port"];
                bool is_alive = node_data["alive"];

                std::lock_guard<std::mutex> lock(nodes_mutex_);
                if (nodes_.find(node_id) == nodes_.end()) {
                    // New node discovered
                    auto new_node = std::make_shared<Node>(node_id, ip, port);
                    if (is_alive) {
                        new_node->markAlive();
                    }
                    else {
                        new_node->markDead();
                    }
                    nodes_[node_id] = new_node;
                }
                else {
                    // Update existing node
                    if (is_alive) {
                        nodes_[node_id]->markAlive();
                    }
                    else {
                        nodes_[node_id]->markDead();
                    }
                }
            }
        }

    private:
        void gossipLoop() {
            while (running_) {
                checkNodeHeartbeats();
                sendGossipMessages();
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }

        void checkNodeHeartbeats() {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            auto now = std::chrono::steady_clock::now();

            for (const auto& pair : nodes_) {
                const auto& node = pair.second;
                if (node->getId() != local_node_->getId()) {
                    auto time_since_heartbeat = std::chrono::duration_cast<std::chrono::seconds>(
                        now - node->getLastHeartbeat()).count();

                    if (time_since_heartbeat > 10 && node->isAlive()) {
                        // Node considered dead after 10 seconds without heartbeat
                        std::cout << "Node " << node->getId() << " is now considered dead" << std::endl;
                        node->markDead();
                        // Trigger rebalancing if needed
                    }
                }
            }
        }

        void sendGossipMessages() {
            // Select random nodes to gossip with
            std::vector<std::shared_ptr<Node>> target_nodes;
            {
                std::lock_guard<std::mutex> lock(nodes_mutex_);
                for (const auto& pair : nodes_) {
                    const auto& node = pair.second;
                    if (node->getId() != local_node_->getId() && node->isAlive()) {
                        target_nodes.push_back(node);
                    }
                }
            }

            if (target_nodes.empty()) {
                return;
            }

            // Randomly select a subset of nodes
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(target_nodes.begin(), target_nodes.end(), g);

            // Take at most 3 nodes to gossip with
            int gossip_count = std::min(3, static_cast<int>(target_nodes.size()));

            // Create gossip message
            json gossip_data;
            gossip_data["nodes"] = json::array();

            {
                std::lock_guard<std::mutex> lock(nodes_mutex_);
                for (const auto& pair : nodes_) {
                    const auto& node = pair.second;
                    json node_info;
                    node_info["id"] = node->getId();
                    node_info["ip"] = node->getIp();
                    node_info["port"] = node->getPort();
                    node_info["alive"] = node->isAlive();
                    gossip_data["nodes"].push_back(node_info);
                }
            }

            // Send gossip message to selected nodes
            for (int i = 0; i < gossip_count; ++i) {
                sendGossipTo(target_nodes[i], gossip_data);
            }
        }

        void sendGossipTo(std::shared_ptr<Node> target, const json& gossip_data) {
            // In a real implementation, this would use asio to send a message
            // For now, we'll simulate successful sending
            std::cout << "Sending gossip to " << target->getId() << std::endl;

            // In a real implementation, this would be asynchronous
            // target->handleGossipMessage(local_node_->getId(), gossip_data);
        }

        std::shared_ptr<Node> local_node_;
        std::unordered_map<NodeId, std::shared_ptr<Node>> nodes_;
        std::mutex nodes_mutex_;
        std::thread gossip_thread_;
        std::atomic<bool> running_;
    };

    // ---------- 2. Consistent Hashing ----------

    class ConsistentHash {
    public:
        ConsistentHash(int num_virtual_nodes = 256)
            : num_virtual_nodes_(num_virtual_nodes) {
        }

        void addNode(std::shared_ptr<Node> node) {
            std::lock_guard<std::mutex> lock(ring_mutex_);

            // Add virtual nodes to the ring
            for (int i = 0; i < num_virtual_nodes_; ++i) {
                Hash hash = hashFunction(node->getId() + ":" + std::to_string(i));
                ring_[hash] = node;
            }

            node_virtual_nodes_[node->getId()] = num_virtual_nodes_;
        }

        void removeNode(const NodeId& node_id) {
            std::lock_guard<std::mutex> lock(ring_mutex_);

            // Remove virtual nodes from the ring
            for (int i = 0; i < node_virtual_nodes_[node_id]; ++i) {
                Hash hash = hashFunction(node_id + ":" + std::to_string(i));
                ring_.erase(hash);
            }

            node_virtual_nodes_.erase(node_id);
        }

        std::vector<std::shared_ptr<Node>> getNodesForKey(const Key& key, int replica_count) {
            std::vector<std::shared_ptr<Node>> nodes;

            if (ring_.empty()) {
                return nodes;
            }

            std::lock_guard<std::mutex> lock(ring_mutex_);

            Hash hash = hashFunction(key);
            auto it = ring_.lower_bound(hash);

            // If we reached the end, wrap around to the beginning
            if (it == ring_.end()) {
                it = ring_.begin();
            }

            // Keep track of nodes we've already added
            std::map<NodeId, bool> added_nodes;

            // Get the requested number of replicas
            while (nodes.size() < replica_count && added_nodes.size() < node_virtual_nodes_.size()) {
                if (added_nodes.find(it->second->getId()) == added_nodes.end()) {
                    nodes.push_back(it->second);
                    added_nodes[it->second->getId()] = true;
                }

                ++it;
                if (it == ring_.end()) {
                    it = ring_.begin();
                }
            }

            return nodes;
        }

        std::shared_ptr<Node> getPrimaryNodeForKey(const Key& key) {
            auto nodes = getNodesForKey(key, 1);
            if (nodes.empty()) {
                return nullptr;
            }
            return nodes[0];
        }

    private:
        Hash hashFunction(const std::string& str) {
            // Simple MurmurHash implementation
            const uint64_t m = 0xc6a4a7935bd1e995ULL;
            const int r = 47;
            uint64_t h = 0x8445d61a4e774912ULL ^ (str.length() * m);

            const uint64_t* data = (const uint64_t*)str.data();
            const uint64_t* end = data + (str.length() / 8);

            while (data != end) {
                uint64_t k = *data++;

                k *= m;
                k ^= k >> r;
                k *= m;

                h ^= k;
                h *= m;
            }

            const unsigned char* data2 = (const unsigned char*)data;

            switch (str.length() & 7) {
            case 7: h ^= uint64_t(data2[6]) << 48;
            case 6: h ^= uint64_t(data2[5]) << 40;
            case 5: h ^= uint64_t(data2[4]) << 32;
            case 4: h ^= uint64_t(data2[3]) << 24;
            case 3: h ^= uint64_t(data2[2]) << 16;
            case 2: h ^= uint64_t(data2[1]) << 8;
            case 1: h ^= uint64_t(data2[0]);
                h *= m;
            };

            h ^= h >> r;
            h *= m;
            h ^= h >> r;

            return h;
        }

        int num_virtual_nodes_;
        std::map<Hash, std::shared_ptr<Node>> ring_;
        std::unordered_map<NodeId, int> node_virtual_nodes_;
        std::mutex ring_mutex_;
    };

    // ---------- 3. Data Replication ----------

    class ReplicationManager {
    public:
        ReplicationManager(std::shared_ptr<ConsistentHash> consistent_hash,
            std::shared_ptr<ClusterManager> cluster_manager,
            std::shared_ptr<Node> local_node,
            int replication_factor = 3)
            : consistent_hash_(consistent_hash),
            cluster_manager_(cluster_manager),
            local_node_(local_node),
            replication_factor_(replication_factor) {
        }

        // Replicates data to appropriate nodes
        bool replicateData(const Key& key, const Value& value) {
            auto nodes = consistent_hash_->getNodesForKey(key, replication_factor_);
            bool success = false;
            int successful_replicas = 0;
            std::string successful_nodes;

            for (const auto& node : nodes) {
                if (!node->isAlive()) {
                    continue;
                }

                if (sendReplicateRequest(node, key, value)) {
                    successful_replicas++;
                    if (!successful_nodes.empty())
                        successful_nodes += ", ";
                    successful_nodes += node->getId();
                }
            }

            // Quorum-based success criteria (more than half of intended replicas)
            if (successful_replicas > 0 && successful_replicas >= std::min(2, (replication_factor_ / 2) + 1)) {
                success = true;

                // Log the replication status
                if (successful_replicas < replication_factor_) {
                    printTimestampedMessage("Write succeeded with " + std::to_string(successful_replicas) +
                        "/" + std::to_string(replication_factor_) +
                        " replicas (" + successful_nodes + " acknowledged)");
                }
            }

            return success;
        }

        // Handles read requests with consistency level
        std::optional<Value> readData(const Key& key, int consistency_level = 1) {
            auto nodes = consistent_hash_->getNodesForKey(key, replication_factor_);
            std::vector<Value> values;

            for (const auto& node : nodes) {
                if (!node->isAlive()) {
                    continue;
                }

                auto value = requestValueFromNode(node, key);
                if (value.has_value()) {
                    values.push_back(value.value());
                }

                if (values.size() >= consistency_level) {
                    break;
                }
            }

            if (values.size() < consistency_level) {
                return std::nullopt;
            }

            // In a real implementation, we might need to resolve conflicts here
            return values[0];
        }

        // Handle read repair - reconcile differences between replicas
        void performReadRepair(const Key& key, const Value& most_recent_value) {
            auto nodes = consistent_hash_->getNodesForKey(key, replication_factor_);
            int repair_count = 0;

            for (const auto& node : nodes) {
                if (!node->isAlive()) {
                    continue;
                }

                auto current_value = requestValueFromNode(node, key);

                // If node doesn't have the value or has a different value
                if (!current_value.has_value() || current_value.value() != most_recent_value) {
                    sendReplicateRequest(node, key, most_recent_value);
                    repair_count++;
                }
            }

            if (repair_count > 0) {
                printTimestampedMessage("Read repair: Updated " + std::to_string(repair_count) +
                    " replicas for key '" + key + "'");
            }
        }

    private:
        bool sendReplicateRequest(std::shared_ptr<Node> node, const Key& key, const Value& value) {
            // In a real implementation, this would use asio to send a network request
            std::cout << "Replicating key " << key << " to node " << node->getId() << std::endl;

            // Simulated success
            return true;
        }

        std::optional<Value> requestValueFromNode(std::shared_ptr<Node> node, const Key& key) {
            // In a real implementation, this would use asio to send a network request
            std::cout << "Requesting key " << key << " from node " << node->getId() << std::endl;

            // Simulated value (in a real implementation, we'd get actual data from the node)
            return std::optional<Value>("simulated_value_for_" + key);
        }

        std::shared_ptr<ConsistentHash> consistent_hash_;
        std::shared_ptr<ClusterManager> cluster_manager_;
        std::shared_ptr<Node> local_node_;
        int replication_factor_;
    };

    // ---------- 4. Leader Election ----------

    class LeaderElection {
    public:
        LeaderElection(NodeId local_node_id, std::shared_ptr<ClusterManager> cluster_manager)
            : local_node_id_(local_node_id),
            cluster_manager_(cluster_manager),
            current_leader_id_(""),
            running_(false) {
        }

        ~LeaderElection() {
            stop();
        }

        void start() {
            running_ = true;
            election_thread_ = std::thread(&LeaderElection::electionLoop, this);
        }

        void stop() {
            running_ = false;
            if (election_thread_.joinable()) {
                election_thread_.join();
            }
        }

        bool isLeader() const {
            return current_leader_id_ == local_node_id_;
        }

        NodeId getCurrentLeader() const {
            return current_leader_id_;
        }

    private:
        void electionLoop() {
            while (running_) {
                if (current_leader_id_.empty() ||
                    !cluster_manager_->getNode(current_leader_id_) ||
                    !cluster_manager_->getNode(current_leader_id_)->isAlive()) {
                    // We need to elect a new leader
                    initiateElection();
                }

                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }

        void initiateElection() {
            std::cout << "Initiating leader election" << std::endl;

            // Simple election algorithm: highest node ID becomes leader
            auto alive_nodes = cluster_manager_->getAliveNodes();
            if (alive_nodes.empty()) {
                current_leader_id_ = "";
                return;
            }

            NodeId highest_id = alive_nodes[0]->getId();
            for (const auto& node : alive_nodes) {
                if (node->getId() > highest_id) {
                    highest_id = node->getId();
                }
            }

            current_leader_id_ = highest_id;
            std::cout << "New leader elected: " << current_leader_id_ << std::endl;

            // Broadcast the election result
            broadcastElectionResult();
        }

        void broadcastElectionResult() {
            // In a real implementation, this would inform all nodes of the new leader
            std::cout << "Broadcasting leader election result: " << current_leader_id_ << std::endl;
        }

        NodeId local_node_id_;
        std::shared_ptr<ClusterManager> cluster_manager_;
        NodeId current_leader_id_;
        std::thread election_thread_;
        std::atomic<bool> running_;
    };

    // ---------- 5. Storage Engine ----------

    class StorageEngine {
    public:
        StorageEngine() {}

        void put(const Key& key, const Value& value) {
            std::lock_guard<std::mutex> lock(data_mutex_);
            data_[key] = value;
        }

        std::optional<Value> get(const Key& key) {
            std::lock_guard<std::mutex> lock(data_mutex_);
            auto it = data_.find(key);
            if (it != data_.end()) {
                return it->second;
            }
            return std::nullopt;
        }

        bool remove(const Key& key) {
            std::lock_guard<std::mutex> lock(data_mutex_);
            return data_.erase(key) > 0;
        }

        size_t size() const {
            std::lock_guard<std::mutex> lock(data_mutex_);
            return data_.size();
        }

    private:
        std::unordered_map<Key, Value> data_;
        mutable std::mutex data_mutex_;
    };

    // ---------- 6. Client API ----------

    class ClientHandler {
    public:
        ClientHandler(std::shared_ptr<StorageEngine> storage_engine,
            std::shared_ptr<ConsistentHash> consistent_hash,
            std::shared_ptr<ReplicationManager> replication_manager,
            std::shared_ptr<Node> local_node)
            : storage_engine_(storage_engine),
            consistent_hash_(consistent_hash),
            replication_manager_(replication_manager),
            local_node_(local_node) {
        }

        bool put(const Key& key, const Value& value) {
            // Check if this node is responsible for the key
            auto primary_node = consistent_hash_->getPrimaryNodeForKey(key);

            if (!primary_node) {
                return false;
            }

            if (primary_node->getId() == local_node_->getId()) {
                // We are the primary node for this key
                storage_engine_->put(key, value);

                // Replicate to other nodes
                return replication_manager_->replicateData(key, value);
            }
            else {
                // Forward to primary node
                return forwardPutRequest(primary_node, key, value);
            }
        }

        std::optional<Value> get(const Key& key, int consistency_level = 1) {
            // Try to get the value from the replication manager with the specified consistency level
            auto value = replication_manager_->readData(key, consistency_level);

            if (value.has_value()) {
                // Perform read repair in the background
                std::thread([this, key, value = value.value()]() {
                    replication_manager_->performReadRepair(key, value);
                    }).detach();
            }

            return value;
        }

        bool remove(const Key& key) {
            // Check if this node is responsible for the key
            auto primary_node = consistent_hash_->getPrimaryNodeForKey(key);

            if (!primary_node) {
                return false;
            }

            if (primary_node->getId() == local_node_->getId()) {
                // We are the primary node for this key
                bool success = storage_engine_->remove(key);

                // Replicate removal to other nodes
                if (success) {
                    // In a real implementation, we'd send delete requests to replicas
                }

                return success;
            }
            else {
                // Forward to primary node
                return forwardRemoveRequest(primary_node, key);
            }
        }

    private:
        bool forwardPutRequest(std::shared_ptr<Node> node, const Key& key, const Value& value) {
            // In a real implementation, this would use asio to send a network request
            std::cout << "Forwarding PUT request for key " << key << " to node " << node->getId() << std::endl;

            // Simulated success
            return true;
        }

        bool forwardRemoveRequest(std::shared_ptr<Node> node, const Key& key) {
            // In a real implementation, this would use asio to send a network request
            std::cout << "Forwarding REMOVE request for key " << key << " to node " << node->getId() << std::endl;

            // Simulated success
            return true;
        }

        std::shared_ptr<StorageEngine> storage_engine_;
        std::shared_ptr<ConsistentHash> consistent_hash_;
        std::shared_ptr<ReplicationManager> replication_manager_;
        std::shared_ptr<Node> local_node_;
    };

    // ---------- Main Node Class ----------

    class KeyValueNode {
    public:
        KeyValueNode(const NodeId& id, const std::string& ip, int port, int replication_factor = 3)
            : running_(false) {
            // Initialize components
            local_node_ = std::make_shared<Node>(id, ip, port);
            cluster_manager_ = std::make_shared<ClusterManager>(local_node_);
            consistent_hash_ = std::make_shared<ConsistentHash>();
            storage_engine_ = std::make_shared<StorageEngine>();

            // Pass the local_node_ to the ReplicationManager
            replication_manager_ = std::make_shared<ReplicationManager>(
                consistent_hash_, cluster_manager_, local_node_, replication_factor
            );

            leader_election_ = std::make_shared<LeaderElection>(id, cluster_manager_);
            client_handler_ = std::make_shared<ClientHandler>(
                storage_engine_, consistent_hash_, replication_manager_, local_node_
            );

            // Add ourselves to the cluster
            cluster_manager_->addNode(local_node_);
            consistent_hash_->addNode(local_node_);
        }

        ~KeyValueNode() {
            stop();
        }

        void start() {
            if (running_) {
                return;
            }

            running_ = true;

            // Start components
            cluster_manager_->start();
            leader_election_->start();

            // Start networking
            startNetworking();
        }

        void stop() {
            if (!running_) {
                return;
            }

            running_ = false;

            // Stop components
            leader_election_->stop();
            cluster_manager_->stop();

            // Stop networking
            stopNetworking();
        }

        // Join the cluster by contacting a known node
        bool joinCluster(const std::string& seed_ip, int seed_port) {
            std::cout << "Joining cluster via seed node " << seed_ip << ":" << seed_port << std::endl;

            // In a real implementation, this would contact the seed node
            // and receive information about the cluster

            // For example, we might receive a list of nodes and add them to our cluster
            std::vector<std::shared_ptr<Node>> seed_nodes;
            for (const auto& node : seed_nodes) {
                cluster_manager_->addNode(node);
                consistent_hash_->addNode(node);
            }

            return true;
        }

        // Client API methods
        bool put(const Key& key, const Value& value) {
            return client_handler_->put(key, value);
        }

        std::optional<Value> get(const Key& key, int consistency_level = 1) {
            return client_handler_->get(key, consistency_level);
        }

        bool remove(const Key& key) {
            return client_handler_->remove(key);
        }

        int getRecoveredUpdatesCount() const {
            return missed_updates_recovered_.load();
        }

        void incrementRecoveredUpdates() {
            missed_updates_recovered_++;
        }

    private:
        void startNetworking() {
            // In a real implementation, this would start a network server
            // to handle requests from clients and other nodes
            std::cout << "Starting network server on " << local_node_->getIp()
                << ":" << local_node_->getPort() << std::endl;
        }

        void stopNetworking() {
            // In a real implementation, this would stop the network server
            std::cout << "Stopping network server" << std::endl;
        }

        std::shared_ptr<Node> local_node_;
        std::shared_ptr<ClusterManager> cluster_manager_;
        std::shared_ptr<ConsistentHash> consistent_hash_;
        std::shared_ptr<StorageEngine> storage_engine_;
        std::shared_ptr<ReplicationManager> replication_manager_;
        std::shared_ptr<LeaderElection> leader_election_;
        std::shared_ptr<ClientHandler> client_handler_;
        std::atomic<bool> running_;
        std::atomic<int> missed_updates_recovered_{ 0 };
    };

} // namespace dkv

// ---------- Main Example ----------

void printTimestampedMessage(const std::string& message) {
    static std::mutex cout_mutex;
    std::lock_guard<std::mutex> lock(cout_mutex);

    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - std::chrono::system_clock::from_time_t(now_time_t)).count();

    std::tm tm_buf;
#ifdef _WIN32
    localtime_s(&tm_buf, &now_time_t);
#else
    localtime_r(&now_time_t, &tm_buf);
#endif

    char time_str[20];
    std::strftime(time_str, sizeof(time_str), "%H:%M:%S", &tm_buf);

    std::cout << "[" << time_str << "." << std::setfill('0') << std::setw(3) << milliseconds << "] "
        << message << std::endl;
}

// Added struct to track metrics
struct SystemMetrics {
    std::map<std::string, int> operations_count;
    std::map<std::string, std::chrono::milliseconds> latencies;

    void recordOperation(const std::string& op) {
        operations_count[op]++;
    }

    void recordLatency(const std::string& op, std::chrono::milliseconds latency) {
        if (latencies.find(op) == latencies.end() || latencies[op] < latency) {
            latencies[op] = latency;
        }
    }

    void printMetrics() {
        std::cout << "\n===== SYSTEM METRICS =====\n";
        std::cout << "Operations performed:\n";
        for (const auto& op : operations_count) {
            std::cout << "  - " << op.first << ": " << op.second << " operations\n";
        }

        std::cout << "Maximum operation latencies:\n";
        for (const auto& latency : latencies) {
            std::cout << "  - " << latency.first << ": " << latency.second.count() << "ms\n";
        }
        std::cout << "=========================\n";
    }
};

int main() {
    std::cout << "\n===== DISTRIBUTED KEY-VALUE STORE DEMO =====\n\n";
    SystemMetrics metrics;

    // Create multiple nodes
    printTimestampedMessage("Creating cluster with 3 nodes");
    dkv::KeyValueNode node1("node1", "127.0.0.1", 6001);
    dkv::KeyValueNode node2("node2", "127.0.0.1", 6002);
    dkv::KeyValueNode node3("node3", "127.0.0.1", 6003);

    // Start the nodes
    printTimestampedMessage("Starting node1 (127.0.0.1:6001)");
    node1.start();

    printTimestampedMessage("Starting node2 (127.0.0.1:6002)");
    node2.start();

    printTimestampedMessage("Starting node3 (127.0.0.1:6003)");
    node3.start();

    // Have node2 and node3 join node1's cluster
    printTimestampedMessage("Node2 joining the cluster via node1");
    auto start = std::chrono::high_resolution_clock::now();
    bool join_success2 = node2.joinCluster("127.0.0.1", 6001);
    auto end = std::chrono::high_resolution_clock::now();
    metrics.recordOperation("cluster_join");
    metrics.recordLatency("cluster_join", std::chrono::duration_cast<std::chrono::milliseconds>(end - start));

    if (join_success2) {
        printTimestampedMessage("Node2 successfully joined the cluster");
    }
    else {
        printTimestampedMessage("ERROR: Node2 failed to join the cluster");
    }

    printTimestampedMessage("Node3 joining the cluster via node1");
    start = std::chrono::high_resolution_clock::now();
    bool join_success3 = node3.joinCluster("127.0.0.1", 6001);
    end = std::chrono::high_resolution_clock::now();
    metrics.recordOperation("cluster_join");
    metrics.recordLatency("cluster_join", std::chrono::duration_cast<std::chrono::milliseconds>(end - start));

    if (join_success3) {
        printTimestampedMessage("Node3 successfully joined the cluster");
    }
    else {
        printTimestampedMessage("ERROR: Node3 failed to join the cluster");
    }

    // Wait for leader election to settle
    printTimestampedMessage("Waiting for cluster to stabilize...");
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Store multiple data items
    std::map<std::string, std::string> test_data;
    test_data["user:1001"] = "John Doe";
    test_data["user:1002"] = "Jane Smith";
    test_data["product:5001"] = "Smartphone";
    test_data["product:5002"] = "Laptop";
    test_data["order:9001"] = "Order #9001: 2 items, $1250.00";

    printTimestampedMessage("--- WRITING DATA TO CLUSTER ---");
    // Fixed for pre-C++17: replace structured binding with traditional iterator
    for (const auto& item : test_data) {
        const std::string& key = item.first;
        const std::string& value = item.second;

        start = std::chrono::high_resolution_clock::now();
        bool success = node1.put(key, value);
        end = std::chrono::high_resolution_clock::now();
        auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        metrics.recordOperation("write");
        metrics.recordLatency("write", latency);

        if (success) {
            printTimestampedMessage("Successfully wrote key '" + key + "' with value '" + value +
                "' (latency: " + std::to_string(latency.count()) + "ms)");
        }
        else {
            printTimestampedMessage("ERROR: Failed to write key '" + key + "'");
        }
    }

    // Retrieve data from different nodes to demonstrate replication
    printTimestampedMessage("\n--- READING DATA FROM DIFFERENT NODES ---");
    std::vector<std::string> test_keys;
    test_keys.push_back("user:1001");
    test_keys.push_back("product:5001");
    test_keys.push_back("order:9001");

    for (const auto& key : test_keys) {
        // Read from node 1
        start = std::chrono::high_resolution_clock::now();
        auto value1 = node1.get(key);
        end = std::chrono::high_resolution_clock::now();
        auto latency1 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        metrics.recordOperation("read");
        metrics.recordLatency("read", latency1);

        // Read from node 2
        start = std::chrono::high_resolution_clock::now();
        auto value2 = node2.get(key);
        end = std::chrono::high_resolution_clock::now();
        auto latency2 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        metrics.recordOperation("read");
        metrics.recordLatency("read", latency2);

        // Read from node 3
        start = std::chrono::high_resolution_clock::now();
        auto value3 = node3.get(key);
        end = std::chrono::high_resolution_clock::now();
        auto latency3 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        metrics.recordOperation("read");
        metrics.recordLatency("read", latency3);

        printTimestampedMessage("Key: " + key);
        printTimestampedMessage("  - Value from node1: " + value1.value_or("NOT FOUND") +
            " (latency: " + std::to_string(latency1.count()) + "ms)");
        printTimestampedMessage("  - Value from node2: " + value2.value_or("NOT FOUND") +
            " (latency: " + std::to_string(latency2.count()) + "ms)");
        printTimestampedMessage("  - Value from node3: " + value3.value_or("NOT FOUND") +
            " (latency: " + std::to_string(latency3.count()) + "ms)");
    }

    // Simulate node failure and demonstrate system resilience
    printTimestampedMessage("\n--- SIMULATING NODE FAILURE ---");
    printTimestampedMessage("Stopping node2 (127.0.0.1:6002) [NOTE] The system should now detect node2's failure and route requests to node1/node3");
    node2.stop();

    // Give time for failure detection
    printTimestampedMessage("Waiting for failure detection...");
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Try to read data after node failure
    printTimestampedMessage("\n--- READING DATA AFTER NODE FAILURE ---");
    for (const auto& key : test_keys) {
        start = std::chrono::high_resolution_clock::now();
        auto value1 = node1.get(key);
        end = std::chrono::high_resolution_clock::now();
        auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        metrics.recordOperation("read_after_failure");
        metrics.recordLatency("read_after_failure", latency);

        printTimestampedMessage("Key: " + key);
        printTimestampedMessage("  - Value from node1: " + value1.value_or("NOT FOUND") +
            " (latency: " + std::to_string(latency.count()) + "ms)");

        start = std::chrono::high_resolution_clock::now();
        auto value3 = node3.get(key);
        end = std::chrono::high_resolution_clock::now();
        latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        metrics.recordOperation("read_after_failure");
        metrics.recordLatency("read_after_failure", latency);

        printTimestampedMessage("  - Value from node3: " + value3.value_or("NOT FOUND") +
            " (latency: " + std::to_string(latency.count()) + "ms)");
    }

    // Write data with one node down
    printTimestampedMessage("\n--- WRITING DATA WITH ONE NODE DOWN ---");
    std::string new_key = "emergency:1001";
    std::string new_value = "Emergency contact: 555-123-4567";

    start = std::chrono::high_resolution_clock::now();
    bool write_success = node1.put(new_key, new_value);
    end = std::chrono::high_resolution_clock::now();
    auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    metrics.recordOperation("write_during_failure");
    metrics.recordLatency("write_during_failure", latency);

    if (write_success) {
        printTimestampedMessage("Successfully wrote key '" + new_key + "' with value '" + new_value +
            "' despite node failure (latency: " + std::to_string(latency.count()) + "ms)");
    }
    else {
        printTimestampedMessage("ERROR: Failed to write key '" + new_key + "' during node failure");
    }

    // Read back the value to verify
    start = std::chrono::high_resolution_clock::now();
    auto emergency_value = node3.get(new_key);
    end = std::chrono::high_resolution_clock::now();
    latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    printTimestampedMessage("Reading back emergency data from node3: " +
        emergency_value.value_or("NOT FOUND") +
        " (latency: " + std::to_string(latency.count()) + "ms)");

    // Recover the failed node
    printTimestampedMessage("\n--- RECOVERING FAILED NODE ---");
    printTimestampedMessage("Restarting node2 (127.0.0.1:6002)");
    node2.start();
    node2.joinCluster("127.0.0.1", 6001);

    // Allow time for recovery and data synchronization
    printTimestampedMessage("Waiting for node recovery and data sync...");
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Verify data on recovered node
    printTimestampedMessage("\n--- VERIFYING DATA ON RECOVERED NODE ---");

    // Read keys to trigger read repair
    for (const auto& key_pair : test_data) {
        const auto& key = key_pair.first;
        node2.get(key); // This will trigger read repair in the background
    }

    // Also check our emergency key
    node2.get(new_key);

    // Let read repair complete
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Show recovery stats
    printTimestampedMessage("Node2 sync complete: Recovered " +
        std::to_string(node2.getRecoveredUpdatesCount()) +
        " missing keys from replicas");

    start = std::chrono::high_resolution_clock::now();
    auto recovered_value = node2.get(new_key);
    end = std::chrono::high_resolution_clock::now();
    latency = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    metrics.recordOperation("read_after_recovery");
    metrics.recordLatency("read_after_recovery", latency);

    printTimestampedMessage("Reading emergency data from recovered node2: " +
        recovered_value.value_or("NOT FOUND") +
        " (latency: " + std::to_string(latency.count()) + "ms)");

    // Display performance metrics
    metrics.printMetrics();

    // Clean up
    printTimestampedMessage("\n--- SHUTTING DOWN CLUSTER ---");
    printTimestampedMessage("Stopping node1");
    node1.stop();
    printTimestampedMessage("Stopping node2");
    node2.stop();
    printTimestampedMessage("Stopping node3");
    node3.stop();

    printTimestampedMessage("Cluster shutdown complete");
    std::cout << "\n===== DEMO COMPLETED =====\n";

    return 0;
}
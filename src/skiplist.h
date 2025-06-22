// skip_list.h

#pragma once

#include <atomic>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <vector>
#include <utility>

template<typename K, typename V>
class Node {
public:
    std::pair<K, V> data_;
    std::vector<std::atomic<Node*>> next_;
    std::atomic<bool> removed{ false };
    Node(std::pair<K, V> data, int level)
        : data_(std::move(data)), next_(level) {

        for (auto& n : next_)
            n.store(nullptr, std::memory_order_relaxed);
    }
};

template<typename K, typename V>
class SkipList {
    static constexpr int MAX_LEVEL = 20;
    static constexpr float P = 0.5f;

    Node<K, V>* head_;
    std::atomic<size_t> count_{ 0 };

    static int randomLevel() {
        thread_local std::mt19937_64 gen{ std::random_device{}() };
        thread_local std::uniform_real_distribution<float> dist(0.f, 1.f);
        int lvl = 1;
        while (dist(gen) < P && lvl < MAX_LEVEL) ++lvl;
        return lvl;
    }

    void findNodes(const K& key, int max_level, Node<K, V>* start_node, Node<K, V>* preds[], Node<K, V>* succs[]) const {
        Node<K, V>* curr = start_node;
        for (int lvl = MAX_LEVEL - 1; lvl >= max_level; --lvl) {
            Node<K, V>* next = curr->next_[lvl].load(std::memory_order_acquire);
            while (next && next->data_.first <= key) {
                curr = next;
                next = curr->next_[lvl].load(std::memory_order_acquire);
            }
            preds[lvl] = curr;
            succs[lvl] = next;
        }
    }

    Node<K, V>* lowerBoundNode(const K& key) const {
        Node<K, V>* curr = head_;
        for (int lvl = MAX_LEVEL - 1; lvl >= 0; --lvl) {
            Node<K, V>* next = curr->next_[lvl].load(std::memory_order_relaxed);
            while (next && next->data_.first < key) {
                curr = next;
                next = curr->next_[lvl].load(std::memory_order_relaxed);
            }
        }
        curr = curr->next_[0].load(std::memory_order_acquire);

        while (curr && curr->removed.load(std::memory_order_acquire)) {
            curr = curr->next_[0].load(std::memory_order_acquire);
        }
        return curr;
    }

public:

    class const_iterator {
        const Node<K, V>* node_;
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<K, V>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;

        explicit const_iterator(const Node<K, V>* node) : node_(node) {}

        reference operator*() const { return node_->data_; }
        pointer operator->() const { return &node_->data_; }

        const_iterator& operator++() {
            if (node_) {
                do {
                    node_ = node_->next_[0].load(std::memory_order_relaxed);
                } while (node_ && node_->removed.load(std::memory_order_relaxed));

            }
            return *this;
        }

        const_iterator operator++(int) {
            const_iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const const_iterator& other) const { return node_ == other.node_; }
        bool operator!=(const const_iterator& other) const { return node_ != other.node_; }
    };


    SkipList()
        : head_{ new Node<K, V>({}, MAX_LEVEL) } {}

    ~SkipList() {
        Node<K, V>* curr = head_;
        while (curr) {
            Node<K, V>* next = curr->next_[0].load(std::memory_order_relaxed);
            delete curr;
            curr = next;
        }
    }
    size_t size() const {
        return count_.load(std::memory_order_relaxed);
    }

    void insert(const std::pair<K, V> data) {
        Node<K, V>* preds[MAX_LEVEL];
        Node<K, V>* succs[MAX_LEVEL];

        const int lvl = randomLevel();
        Node<K, V>* node = new Node<K, V>(std::move(data), MAX_LEVEL);
        auto& key = node->data_.first;
        bool was_removed = false;
        auto start_node = head_;
        while (true) {
            findNodes(key, 0, start_node, preds, succs);
            start_node = preds[0];
            if (!was_removed && preds[0] && preds[0]->data_.first == key) {
                bool expected = false;
                if (!preds[0]->removed
                    .compare_exchange_strong(expected, true,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
                    continue;
                }
                was_removed = true;
            }
            node->next_[0].store(succs[0], std::memory_order_release);
            if (preds[0]->next_[0].compare_exchange_strong(
                succs[0], node,
                std::memory_order_acq_rel,
                std::memory_order_relaxed)) {

                break;
            };
        }

        for (int i = 1; i < lvl; ++i) {
            while (true) {
                node->next_[i].store(succs[i], std::memory_order_release);
                if (preds[i]->next_[i].compare_exchange_strong(
                    succs[i], node,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                    break;
                }
                findNodes(key, i, start_node, preds, succs);

            }
        }

        if (!was_removed) {
            count_.fetch_add(1, std::memory_order_relaxed);
        }
    }


    const_iterator find(const K& key) const {
        Node<K, V>* curr = lowerBoundNode(key);
        if (curr && curr->data_.first == key)
            return const_iterator(curr);
        return cend();
    }

    const_iterator lower_bound(const K& key) const {
        return const_iterator(lowerBoundNode(key));
    }

    const_iterator begin() const {
        if (!head_->next_[0].load(std::memory_order_relaxed)) {
            return end();
        }
        auto curr = head_->next_[0].load(std::memory_order_relaxed);
        while (curr && curr->removed.load(std::memory_order_relaxed)) {
            curr = curr->next_[0].load(std::memory_order_relaxed);
        }
        return const_iterator(curr);
    }
    const_iterator end() const { return const_iterator(nullptr); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }
};

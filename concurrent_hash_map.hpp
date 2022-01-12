#include "sebr_local.hpp"

using namespace sebr;
template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
class ConcurrentHashMap final : public ConcurrentBridge<ConcurrentHashMap<K, V, Hash>> {
private:
    class DelayDispose {
    public:
        DelayDispose() : ptr(nullptr) {}

        ~DelayDispose() {
            if (ptr != nullptr) {
                ptr();
            }
        }

        std::function<void()> ptr;
    };

    static const int MOVED = -1;    // hash for forwarding nodes
    static const int TREEBIN = -2;  // hash for roots of trees
    static const int RESERVED = -3; // hash for transient reservations
    static const int HASH_BITS = 0x7fffffff;
    unsigned int NCPU = std::thread::hardware_concurrency();
    static const int DEFAULT_CAPACITY = 16;
    static const int MAXIMUM_CAPACITY = 1 << 30;

    /**
   * Minimum number of rebinnings per transfer step. Ranges are
   * subdivided to allow multiple resizer threads.  This value
   * serves as a lower bound to avoid resizers encountering
   * excessive memory contention.  The value should be at least
   * DEFAULT_CAPACITY.
   */
    static const int MIN_TRANSFER_STRIDE = 16;

    /**
   * The number of bits used for generation stamp in sizeCtl.
   * Must be at least 6 for 32bit arrays.
   */
    static const int RESIZE_STAMP_BITS = 16;

    /**
   * The maximum number of threads that can help resize.
   * Must fit in 32 - RESIZE_STAMP_BITS bits.
   */
    static const int MAX_RESIZERS = (1 << (32 - RESIZE_STAMP_BITS)) - 1;

    /**
   * The bit shift for recording size stamp in sizeCtl.
   */
    static const int RESIZE_STAMP_SHIFT = 32 - RESIZE_STAMP_BITS;

    /**
   * The bin count threshold for using a tree rather than list for a
   * bin.  Bins are converted to trees when adding an element to a
   * bin with at least this many nodes. The value must be greater
   * than 2, and should be at least 8 to mesh with assumptions in
   * tree removal about conversion back to plain bins upon
   * shrinkage.
   */
    static const int TREEIFY_THRESHOLD = 8;

    /**
   * The bin count threshold for untreeifying a (split) bin during a
   * resize operation. Should be less than TREEIFY_THRESHOLD, and at
   * most 6 to mesh with shrinkage detection under removal.
   */
    static const int UNTREEIFY_THRESHOLD = 6;

    /**
   * The smallest table capacity for which bins may be treeified.
   * (Otherwise the table is resized if too many nodes in a bin.)
   * The value should be at least 4 * TREEIFY_THRESHOLD to avoid
   * conflicts between resizing and treeification thresholds.
   */
    static const int MIN_TREEIFY_CAPACITY = 64;

    class Node;
    class RecSomeNode : public ReclaimBridge /*, public Stock<RecSomeNode, 1000>*/ {
    public:
        Node* first;

    public:
        RecSomeNode(Node* first) : first(first) {}

        void reclaim() {
            Node* temp = first;
            while (temp != nullptr) {
                Node* next = temp->next;
                delete temp;
                temp = next;
            }
        }
    };

    class TreeBin;
    class RecTreeBin : public ReclaimBridge /*, public Stock<RecTreeBin, 1000>*/ {
    public:
        TreeBin* treeBin;

    public:
        RecTreeBin(TreeBin* treeBin) : treeBin(treeBin) {}

        void reclaim() {
            if (treeBin != nullptr) {
                TreeNode* link_start = treeBin->first;
                delete treeBin;

                Node* temp = link_start;
                while (temp != nullptr) {
                    // assert(dynamic_cast<ForwardingObject*> (temp) == nullptr);
                    Node* next = temp->next;
                    delete temp;
                    temp = next;
                }
            }
        }
    };

    class RecLinkedNode : public ReclaimBridge /*, public Stock<RecLinkedNode, 1000>*/ {
    public:
        // friend class ConcurrentHashMap;
        Node* link_start;
        Node* link_end;

    public:
        RecLinkedNode(Node* link_start, Node* link_end) : link_start(link_start), link_end(link_end) {}

        void reclaim() {
            Node* temp = link_start;
            while (temp != link_end) {
                // assert(dynamic_cast<ForwardingObject*> (temp) == nullptr);
                Node* next = temp->next;
                delete temp;
                temp = next;
            }
        }
    };

    class TreeNode;
    class RecPartialTree : public ReclaimBridge /*, public Stock<RecPartialTree, 1000>*/ {
    public:
        // friend class ConcurrentHashMap;
        TreeBin* tree_bin;
        Node* lo;
        Node* hi;
        static const int len = 2;

    public:
        RecPartialTree(TreeBin* tree_bin, TreeNode* lo, TreeNode* hi) : tree_bin(tree_bin), lo(lo), hi(hi) {}

        void reclaim() {
            if (tree_bin != nullptr) {
                TreeNode* link_start = tree_bin->first;
                delete tree_bin;

                Node* temp = link_start;
                while (temp != nullptr) {
                    // assert(dynamic_cast<ForwardingObject*> (temp) == nullptr);
                    Node* next = temp->next;
                    delete temp;
                    temp = next;
                }
            }

            Node* nodes[len] = {lo, hi};
            for (Node* node : nodes) {
                if (node != nullptr) {
                    Node* temp = node;
                    while (temp != nullptr) {
                        Node* next = temp->next;
                        delete temp;
                        temp = next;
                    }
                }
            }
        }
    };

    class BucketTable;
    class RecForwardingTable : public ReclaimBridge /*, public Stock<RecForwardingTable, 1000>*/ {
    public:
        // friend class ConcurrentHashMap;
        BucketTable* table;

    public:
        RecForwardingTable(BucketTable* table) : table(table) {}

        void reclaim() { delete table; }
    };

    class Node {
    public:
        // friend class ConcurrentHashMap;
        int hash;
        K key;
        std::atomic<V*> val;
        std::atomic<Node*> next;
        bool shallow;

    public:
        Node(int hash, const K& key, const V& val)
                : hash(hash), key(key), val(new V(val)), next(nullptr), shallow(true) {}

        Node(int hash, const K& key, const V& val, Node* next)
                : hash(hash), key(key), val(new V(val)), next(next), shallow(true) {}

        Node(int hash, const K& key, V* val) : hash(hash), key(key), val(val), next(nullptr), shallow(true) {}

        Node(int hash, const K& key, V* val, Node* next) : hash(hash), key(key), val(val), next(next), shallow(true) {}

        virtual ~Node() { // To Improve
            if (!shallow) {
                delete val.load();
                // delete val;
            }
        }

        virtual Node* find(int h, const K& k) {
            // never use node's find
            // use loop with node's next directly.
            Node* e = this;
            do {
                if (e->hash == h && (e->key) == k) {
                    return e;
                }
            } while ((e = e->next.load()) != nullptr);
            return nullptr;
        }
    };

    class ForwardingObject;
    class BucketTable {
    public:
        // friend class ConcurrentHashMap;
        std::atomic<Node*>* tableArray;
        std::mutex* lock_levels;
        int length;
        std::optional<ForwardingObject> share;

    public:
        BucketTable(int n)
                : tableArray(new std::atomic<Node*>[n]()), lock_levels(new std::mutex[n]()), length(n), share() {}

        ~BucketTable() {
            for (int i = 0; i < length; ++i) {
                Node* node = tableArray[i].load();
                if (node == nullptr) continue;
                if (ForwardingObject* forwardNode = dynamic_cast<ForwardingObject*>(node)) {
                    // delete forwardNode; It's someting to do by delete share.
                } else if (TreeBin* treeBin = dynamic_cast<TreeBin*>(node)) {
                    TreeNode* link_start = treeBin->first;
                    delete treeBin;
                    Node* temp = link_start;
                    while (temp != nullptr) {
                        Node* next = temp->next;
                        temp->shallow = false;
                        delete temp;
                        temp = next;
                    }
                } else {
                    Node* temp = node;
                    while (temp != nullptr) {
                        Node* next = temp->next;
                        temp->shallow = false;
                        delete temp;
                        temp = next;
                    }
                }
            }

            delete[] tableArray;
            delete[] lock_levels;

            // delete share;
        }
    };

    class ForwardingObject : public Node {
    public:
        // friend class ConcurrentHashMap<K, V>;
        BucketTable* nextTable;
        const static K default_k();
        const static V default_v();

    public:
        ForwardingObject(BucketTable* nextTable) : Node(MOVED, K(), V()), nextTable(nextTable) {
            this->shallow = false;
        }

        ~ForwardingObject() {}

        Node* find(int h, const K& k) {
            // loop to avoid arbitrarily deep recursion on forwarding nodes
            for (BucketTable* localTable = nextTable;;) {
                Node* e;
                int n, eh;
                std::atomic<Node*>* tab;
                tab = localTable->tableArray;
                n = localTable->length;

                if ((e = tabAt(tab, (n - 1) & h)) == nullptr) return nullptr;

                if ((eh = e->hash) < 0) {
                    if (ForwardingObject* fe = (dynamic_cast<ForwardingObject*>(e))) {
                        localTable = fe->nextTable;
                        continue;
                    } else {
                        return e->find(h, k);
                    }
                }

                for (;;) {
                    if (eh == h && KeyEqual()(e->key, k)) {
                        return e;
                    }

                    if ((e = e->next.load()) == nullptr) return nullptr;
                }
            }
        }
    };
    //    const K ForwardingObject::default_k;
    //    const V ForwardingObject::default_v;

    class TreeNode : public Node {
    public:
        // friend class ConcurrentHashMap;
        std::atomic<TreeNode*> parent; // red-black tree links
        std::atomic<TreeNode*> left;
        std::atomic<TreeNode*> right;
        std::atomic<TreeNode*> prev; // needed to unlink next upon deletion
        std::atomic<bool> red;

    public:
        TreeNode(int hash, const K& key, const V& val, Node* next, TreeNode* parent)
                : Node(hash, key, val, next),
                  parent(parent),
                  left(nullptr),
                  right(nullptr),
                  prev(nullptr),
                  red(false) {}

        TreeNode(int hash, const K& key, V* val, Node* next, TreeNode* parent)
                : Node(hash, key, val, next),
                  parent(parent),
                  left(nullptr),
                  right(nullptr),
                  prev(nullptr),
                  red(false) {}

        Node* find(int h, const K& k) {
            // never use find
            // use findTreeNode directly in TreeBin
            return findTreeNode(h, k);
        }

        /**
     * Returns the TreeNode (or nullptr if not found) for the given key
     * starting at given root->
     */
        TreeNode* findTreeNode(size_t h, const K& k) {
            TreeNode* p = this;
            do {
                size_t ph;
                // K* pk;
                TreeNode* pl = p->left;
                TreeNode* pr = p->right;
                TreeNode* q = nullptr;
                if ((ph = p->hash) > h)
                    p = pl;
                else if (ph < h)
                    p = pr;
                else if (KeyEqual()(p->key, k)) {
                    return p;
                } else if (pl == nullptr)
                    p = pr;
                else if (pr == nullptr)
                    p = pl;
                else if ((q = pr->findTreeNode(h, k)) != nullptr)
                    return q;
                else
                    p = pl;
            } while (p != nullptr);
            return nullptr;
        }
    };

    /**
   * TreeNodes used at the heads of bins. TreeBins do not hold user
   * keys or values, but instead point to list of TreeNodes and
   * their root. They also maintain a parasitic read-write lock
   * forcing writers (who hold bin lock) to wait for readers (who do
   * not) to complete before tree restructuring operations.
   */

    class TreeBin : public Node {
    public:
        // friend class ConcurrentHashMap;
        std::atomic<TreeNode*> root;
        std::atomic<TreeNode*> first;
        std::atomic<Blocking*> waiter;
        std::atomic<int> lockState;

        // values for lockState
        static const int WRITER = 1; // set while holding write lock
        static const int WAITER = 2; // set when waiting for write lock
        static const int READER = 4; // increment value for setting read lock

    public:
        /**
     * Creates bin with initial set of nodes headed by b.
     */
        TreeBin(TreeNode* b) : Node(TREEBIN, K(), V()), root(nullptr), first(nullptr), waiter(nullptr), lockState(0) {
            this->shallow = false;
            this->first = b;
            TreeNode* r = nullptr;
            TreeNode* x = b;
            TreeNode* next;
            for (; x != nullptr; x = next) {
                next = static_cast<TreeNode*>(x->next.load());
                x->left = x->right = nullptr;
                if (r == nullptr) {
                    x->parent = nullptr;
                    x->red = false;
                    r = x;
                } else {
                    int h = x->hash;
                    for (TreeNode* p = r;;) {
                        int dir, ph;
                        if ((ph = p->hash) > h)
                            dir = -1;
                        else if (ph < h)
                            dir = 1;
                        else
                            dir = 0;

                        TreeNode* xp = p;
                        if ((p = (dir <= 0) ? p->left : p->right) == nullptr) {
                            x->parent = xp;
                            if (dir <= 0)
                                xp->left = x;
                            else
                                xp->right = x;
                            r = balanceInsertion(r, x);
                            break;
                        }
                    }
                }
            }
            this->root = r;
            bool res = checkInvariants(root);
            assert(res);
        }

        /**
     * Returns matching node or nullptr if none-> Tries to search
     * using tree comparisons from root, but continues linear
     * search when lock not available->
     */
        Node* find(int h, const K& k) {
            for (Node* e = first; e != nullptr;) {
                int s;
                // const K* ek;
                if (((s = lockState.load()) & (WAITER | WRITER)) != 0) {
                    if (e->hash == h && (KeyEqual()(e->key, k))) {
                        return e;
                    }
                    e = e->next;
                } else if (lockState.compare_exchange_strong(s, s + READER)) {
                    TreeNode* r;
                    TreeNode* p;
                    p = ((r = root) == nullptr ? nullptr : r->findTreeNode(h, k));
                    Blocking* w;
                    if (lockState.fetch_add(-READER) == (READER | WAITER) && (w = waiter.load()) != nullptr) {
                        w->unpark();
                    }
                    return p;
                }
            }
            return nullptr;
        }

        /**
     * Acquires write lock for tree restructuring.
     */
        void lockRoot(Pin& keepPin) {
            int s = 0;
            if (!lockState.compare_exchange_strong(s, WRITER)) contendedLock(keepPin); // offload to separate method
        }

        void unlockRoot() { lockState.store(0); }

        void contendedLock(Pin& keepPin) {
            // contend locks
            bool waiting = false;
            Blocking* block = nullptr;
            for (int s;;) {
                if (((s = lockState.load()) & ~WAITER) == 0) {
                    if (lockState.compare_exchange_strong(s, WRITER)) {
                        if (waiting) {
                            waiter.store(nullptr);
                            keepPin.retire<RecSingleNode<Blocking>>(block);
                        }
                        return;
                    }
                } else if ((s & WAITER) == 0) {
                    if (lockState.compare_exchange_strong(s, s | WAITER)) {
                        block = new Blocking();
                        waiting = true;
                        waiter.store(block);
                    }
                } else if (waiting) {
                    block->park();
                }
            }
        }

        Node* putTreeVal(int h, const K& k, const V& v, Pin& keepPin) {
            bool searched = false;
            for (TreeNode* p = root;;) {
                int dir, ph;
                // K* pk;
                if (p == nullptr) {
                    first = root = new TreeNode(h, k, v, nullptr, nullptr);
                    break;
                } else if ((ph = p->hash) > h)
                    dir = -1;
                else if (ph < h)
                    dir = 1;
                else if (KeyEqual()(p->key, k))
                    return p;
                else {
                    if (!searched) {
                        TreeNode* q;
                        TreeNode* ch;
                        searched = true;
                        if (((ch = p->left) != nullptr && (q = ch->findTreeNode(h, k)) != nullptr) ||
                            ((ch = p->right) != nullptr && (q = ch->findTreeNode(h, k)) != nullptr))
                            return q;
                    }
                    dir = 0;
                }

                TreeNode* xp = p;
                if ((p = (dir <= 0) ? p->left : p->right) == nullptr) {
                    TreeNode* x;
                    TreeNode* f = first;
                    first = x = new TreeNode(h, k, v, f, xp);
                    if (f != nullptr) f->prev = x;
                    if (dir <= 0)
                        xp->left = x;
                    else
                        xp->right = x;
                    if (!xp->red)
                        x->red = true;
                    else {
                        lockRoot(keepPin);
                        root = balanceInsertion(root, x);
                        unlockRoot();
                    }
                    break;
                }
            }
            bool res = checkInvariants(root);
            assert(res);
            return nullptr;
        }

        /* ------------------------------------------------------------ */
        // Red-black tree methods, all adapted from CLR
        static TreeNode* rotateLeft(TreeNode* root, TreeNode* p) {
            TreeNode* r;
            TreeNode* pp;
            TreeNode* rl;
            if (p != nullptr && (r = p->right) != nullptr) {
                auto rv = r->left.load();
                p->right = rv;
                if ((rl = rv) != nullptr) {
                    rl->parent = p;
                }
                auto v = p->parent.load();
                r->parent.store(v);
                if ((pp = v) == nullptr)
                    (root = r)->red = false;
                else if (pp->left == p)
                    pp->left = r;
                else
                    pp->right = r;
                r->left = p;
                p->parent = r;
            }
            return root;
        }

        static TreeNode* rotateRight(TreeNode* root, TreeNode* p) {
            TreeNode* l;
            TreeNode* pp;
            TreeNode* lr;
            if (p != nullptr && (l = p->left) != nullptr) {
                auto lv = l->right.load();
                p->left = lv;
                if ((lr = lv) != nullptr) lr->parent = p;
                auto v = p->parent.load();
                l->parent.store(v);
                if ((pp = v) == nullptr)
                    (root = l)->red = false;
                else if (pp->right == p)
                    pp->right = l;
                else
                    pp->left = l;
                l->right = p;
                p->parent = l;
            }
            return root;
        }

        static TreeNode* balanceInsertion(TreeNode* root, TreeNode* x) {
            x->red = true;
            TreeNode* xp;
            TreeNode* xpp;
            TreeNode* xppl;
            TreeNode* xppr;
            for (;;) {
                if ((xp = x->parent) == nullptr) {
                    x->red = false;
                    return x;
                } else if (!xp->red || (xpp = xp->parent) == nullptr)
                    return root;
                if (xp == (xppl = xpp->left)) {
                    if ((xppr = xpp->right) != nullptr && xppr->red) {
                        xppr->red = false;
                        xp->red = false;
                        xpp->red = true;
                        x = xpp;
                    } else {
                        if (x == xp->right) {
                            root = rotateLeft(root, x = xp);
                            xpp = (xp = x->parent) == nullptr ? nullptr : xp->parent.load();
                        }
                        if (xp != nullptr) {
                            xp->red = false;
                            if (xpp != nullptr) {
                                xpp->red = true;
                                root = rotateRight(root, xpp);
                            }
                        }
                    }
                } else {
                    if (xppl != nullptr && xppl->red) {
                        xppl->red = false;
                        xp->red = false;
                        xpp->red = true;
                        x = xpp;
                    } else {
                        if (x == xp->left) {
                            root = rotateRight(root, x = xp);
                            xpp = (xp = x->parent.load()) == nullptr ? nullptr : xp->parent.load();
                        }
                        if (xp != nullptr) {
                            xp->red = false;
                            if (xpp != nullptr) {
                                xpp->red = true;
                                root = rotateLeft(root, xpp);
                            }
                        }
                    }
                }
            }
        }

        /**
     * Checks invariants recursively for the tree of Nodes rooted at t.
     */
        static bool checkInvariants(TreeNode* t) {
            TreeNode* tp = t->parent;
            TreeNode* tl = t->left;
            TreeNode* tr = t->right;
            TreeNode* tb = t->prev;
            TreeNode* tn = static_cast<TreeNode*>(t->next.load());
            if (tb != nullptr && tb->next.load() != t) return false;
            if (tn != nullptr && tn->prev != t) return false;
            if (tp != nullptr && t != tp->left && t != tp->right) return false;
            if (tl != nullptr && (tl->parent != t || tl->hash > t->hash)) return false;
            if (tr != nullptr && (tr->parent != t || tr->hash < t->hash)) return false;
            if (t->red && tl != nullptr && tl->red && tr != nullptr && tr->red) return false;
            if (tl != nullptr && !checkInvariants(tl)) return false;
            if (tr != nullptr && !checkInvariants(tr)) return false;
            return true;
        }

        static TreeNode* balanceDeletion(TreeNode* root, TreeNode* x) {
            TreeNode* xpl;
            TreeNode* xpr;
            for (TreeNode* xp;;) {
                if (x == nullptr || x == root)
                    return root;
                else if ((xp = x->parent) == nullptr) {
                    x->red = false;
                    return x;
                } else if (x->red) {
                    x->red = false;
                    return root;
                } else if ((xpl = xp->left) == x) {
                    if ((xpr = xp->right) != nullptr && xpr->red) {
                        xpr->red = false;
                        xp->red = true;
                        root = rotateLeft(root, xp);
                        xpr = (xp = x->parent) == nullptr ? nullptr : xp->right.load();
                    }
                    if (xpr == nullptr)
                        x = xp;
                    else {
                        TreeNode* sl = xpr->left;
                        TreeNode* sr = xpr->right;
                        if ((sr == nullptr || !sr->red) && (sl == nullptr || !sl->red)) {
                            xpr->red = true;
                            x = xp;
                        } else {
                            if (sr == nullptr || !sr->red) {
                                if (sl != nullptr) sl->red = false;
                                xpr->red = true;
                                root = rotateRight(root, xpr);
                                xpr = (xp = x->parent) == nullptr ? nullptr : xp->right.load();
                            }
                            if (xpr != nullptr) {
                                xpr->red = (xp == nullptr) ? false : xp->red.load();
                                if ((sr = xpr->right) != nullptr) sr->red = false;
                            }
                            if (xp != nullptr) {
                                xp->red = false;
                                root = rotateLeft(root, xp);
                            }
                            x = root;
                        }
                    }
                } else { // symmetric
                    if (xpl != nullptr && xpl->red) {
                        xpl->red = false;
                        xp->red = true;
                        root = rotateRight(root, xp);
                        xpl = (xp = x->parent) == nullptr ? nullptr : xp->left.load();
                    }
                    if (xpl == nullptr)
                        x = xp;
                    else {
                        TreeNode* sl = xpl->left;
                        TreeNode* sr = xpl->right;
                        if ((sl == nullptr || !sl->red) && (sr == nullptr || !sr->red)) {
                            xpl->red = true;
                            x = xp;
                        } else {
                            if (sl == nullptr || !sl->red) {
                                if (sr != nullptr) sr->red = false;
                                xpl->red = true;
                                root = rotateLeft(root, xpl);
                                xpl = (xp = x->parent) == nullptr ? nullptr : xp->left.load();
                            }
                            if (xpl != nullptr) {
                                xpl->red = (xp == nullptr) ? false : xp->red.load();
                                if ((sl = xpl->left) != nullptr) sl->red = false;
                            }
                            if (xp != nullptr) {
                                xp->red = false;
                                root = rotateRight(root, xp);
                            }
                            x = root;
                        }
                    }
                }
            }
        }

        bool flagAndRecTreeNode(bool flag, TreeNode* node, Pin& keepPin, DelayDispose& delayDispose) {
            delayDispose.ptr = [=, &keepPin]() -> void {
                node->shallow = false;
                keepPin.retire<RecSingleNode<TreeNode>>(node);
            };

            return flag;
        }

        /**
     * Removes the given node, that must be present before this
     * call.  This is messier than typical red-black deletion code
     * because we cannot swap the contents of an interior node
     * with a leaf successor that is pinned by "next" pointers
     * that are accessible independently of lock. So instead we
     * swap the tree linkages.
     *
     * @return true if now too small, so should be untreeified
     */
        bool removeTreeNode(TreeNode* p, Pin& keepPin, DelayDispose& delayDispose) {
            // std::cout << "Remove Memory Leak" << std::endl;
            TreeNode* next = static_cast<TreeNode*>(p->next.load());
            TreeNode* pred = p->prev; // unlink traversal pointers
            TreeNode* r;
            TreeNode* rl;
            if (pred == nullptr)
                first = next;
            else
                pred->next = next;
            if (next != nullptr) next->prev = pred;
            if (first == nullptr) {
                root = nullptr;
                return flagAndRecTreeNode(true, p, keepPin, delayDispose);
            }
            if ((r = root) == nullptr || r->right == nullptr || // too small
                (rl = r->left) == nullptr || rl->left == nullptr)
                return flagAndRecTreeNode(true, p, keepPin, delayDispose);

            lockRoot(keepPin);
            TreeNode* replacement;
            TreeNode* pl = p->left;
            TreeNode* pr = p->right;
            if (pl != nullptr && pr != nullptr) {
                TreeNode* s = pr;
                TreeNode* sl;
                while ((sl = s->left) != nullptr) // find successor
                    s = sl;
                bool c = s->red;
                s->red.store(p->red.load());
                p->red = c; // swap colors
                TreeNode* sr = s->right;
                TreeNode* pp = p->parent;
                if (s == pr) { // p was s's direct parent
                    p->parent = s;
                    s->right = p;
                } else {
                    TreeNode* sp = s->parent;
                    if ((p->parent = sp) != nullptr) {
                        if (s == sp->left)
                            sp->left = p;
                        else
                            sp->right = p;
                    }
                    if ((s->right = pr) != nullptr) pr->parent = s;
                }
                p->left = nullptr;
                if ((p->right = sr) != nullptr) sr->parent = p;
                if ((s->left = pl) != nullptr) pl->parent = s;
                if ((s->parent = pp) == nullptr)
                    r = s;
                else if (p == pp->left)
                    pp->left = s;
                else
                    pp->right = s;
                if (sr != nullptr)
                    replacement = sr;
                else
                    replacement = p;
            } else if (pl != nullptr)
                replacement = pl;
            else if (pr != nullptr)
                replacement = pr;
            else
                replacement = p;
            if (replacement != p) {
                TreeNode* pp = p->parent.load();
                replacement->parent.store(pp);
                if (pp == nullptr)
                    r = replacement;
                else if (p == pp->left)
                    pp->left = replacement;
                else
                    pp->right = replacement;
                p->left = p->right = p->parent = nullptr;
            }

            root = (p->red) ? r : balanceDeletion(r, replacement);

            if (p == replacement) { // detach pointers
                TreeNode* pp;
                if ((pp = p->parent) != nullptr) {
                    if (p == pp->left)
                        pp->left = nullptr;
                    else if (p == pp->right)
                        pp->right = nullptr;
                    p->parent = nullptr;
                }
            }
            unlockRoot();
            bool res = checkInvariants(root);
            assert(res);
            return flagAndRecTreeNode(false, p, keepPin, delayDispose);
        }
    };

    static int spread(int h) {
        // unsigned int v = static_cast<unsigned int> (h);
        return (h ^ (static_cast<unsigned int>(h) >> 16)) & HASH_BITS;
    }

    static Node* tabAt(std::atomic<Node*>* tab, int i) { return tab[i].load(); }

    static bool casTabAt(std::atomic<Node*>* tab, int i, Node*& old, Node* newNode) {
        return tab[i].compare_exchange_strong(old, newNode);
    }

    static void setTabAt(std::atomic<Node*>* tab, int i, Node* node) { tab[i].store(node); }

    BucketTable* initTable() {
        BucketTable* localTable;
        int sc;
        while (((localTable = table.load()) == nullptr) || localTable->length == 0) {
            if ((sc = sizeCtl.load()) < 0)
                std::this_thread::yield();
            else if (sizeCtl.compare_exchange_strong(sc, -1)) {
                if ((localTable = table.load()) == nullptr || localTable->length == 0) {
                    int n = (sc > 0) ? sc : DEFAULT_CAPACITY;
                    BucketTable* nt = new BucketTable(n);
                    table.store(localTable = nt);
                    sc = n - (static_cast<unsigned int>(n) >> 2);
                    // sizeCtl.store(sc);
                }
                sizeCtl.store(sc);
                break;
            }
        }
        return localTable;
    }

    static int numberOfLeadingZeros(int i) {
        // HD, Figure 5-6
        if (i == 0) return 32;
        int n = 1;
        if (static_cast<unsigned int>(i) >> 16 == 0) {
            n += 16;
            i <<= 16;
        }
        if (static_cast<unsigned int>(i) >> 24 == 0) {
            n += 8;
            i <<= 8;
        }
        if (static_cast<unsigned int>(i) >> 28 == 0) {
            n += 4;
            i <<= 4;
        }
        if (static_cast<unsigned int>(i) >> 30 == 0) {
            n += 2;
            i <<= 2;
        }
        n -= static_cast<unsigned int>(i) >> 31;
        return n;
    }

    static int resizeStamp(int n) { return numberOfLeadingZeros(n) | (1 << (RESIZE_STAMP_BITS - 1)); }

    void transfer(BucketTable* localTable, Pin& keepPin) {
        int len = localTable->length;

        BucketTable* nt = new BucketTable(len << 1);
        localTable->share.emplace(nt);
        nextTable.store(nt);
        transferIndex.store(len);

        return transfer(localTable, nt, keepPin);
    }

    /**
   * Moves and/or copies the nodes in each bin to new table. See
   * above for explanation.
   */
    void transfer(BucketTable* localTable, BucketTable* nextTab, Pin& keepPin) {
        std::atomic<Node*>* tab = localTable->tableArray;
        int len = localTable->length, stride;
        if ((stride = (NCPU > 1) ? (static_cast<unsigned int>(len) >> 3) / NCPU : len) < MIN_TRANSFER_STRIDE)
            stride = MIN_TRANSFER_STRIDE; // subdivide range

        int nextn = nextTab->length;
        // ForwardingObject* fwd = new ForwardingObject(nextTab);
        bool advance = true;
        bool finishing = false; // to ensure sweep before committing nextTab

        for (int i = 0, bound = 0;;) {
            Node* f;
            int fh;
            while (advance) {
                int nextIndex, nextBound;
                if (--i >= bound || finishing)
                    advance = false;
                else if ((nextIndex = transferIndex.load()) <= 0) {
                    i = -1;
                    advance = false;
                } else if (transferIndex.compare_exchange_strong(
                                   nextIndex, nextBound = (nextIndex > stride ? nextIndex - stride : 0))) {
                    bound = nextBound;
                    i = nextIndex - 1;
                    advance = false;
                }
            }
            if (i < 0 || i >= len || i + len >= nextn) {
                int sc;
                if (finishing) {
                    nextTable.store(nullptr);
                    table.store(nextTab);

                    keepPin.retire<RecForwardingTable>(localTable);

                    sizeCtl.store((len << 1) - (static_cast<unsigned int>(len) >> 1));
                    return;
                }
                sc = sizeCtl.load();
                if (sizeCtl.compare_exchange_strong(sc, sc - 1)) {
                    if ((sc - 2) != resizeStamp(len) << RESIZE_STAMP_SHIFT) return;
                    finishing = advance = true;
                    i = len; // recheck and computes how many bytes should to be
                             // reclaimed
                }
            } else if ((f = tabAt(tab, i)) == nullptr) {
                // ForwardingObject* fwd = new ForwardingObject(nextTab);
                advance = casTabAt(tab, i, f, &*localTable->share);
            } else if ((fh = f->hash) == MOVED) {
                advance = true; // already processed
            } else {
                DelayDispose delayDispose;
                std::lock_guard<std::mutex> control(localTable->lock_levels[i]); // std::cout << "KKKK" << std::endl;
                if (tabAt(tab, i) == f) {
                    Node* ln = nullptr; // std::cout << "JJJJJ" << std::endl;
                    Node* hn = nullptr;
                    // ForwardingObject* fwd = nullptr;
                    if (fh >= 0) {
                        int runBit = fh & len;
                        Node* lastRun = f;
                        for (Node* p = f->next.load(); p != nullptr; p = p->next.load()) {
                            int b = p->hash & len;
                            if (b != runBit) {
                                runBit = b;
                                lastRun = p;
                            }
                        } // std::cout << "PPP" << std::endl;
                        if (runBit == 0) {
                            ln = lastRun;
                            hn = nullptr;
                        } else {
                            hn = lastRun;
                            ln = nullptr;
                        }
                        int bytes_linkn = 0;
                        for (Node* p = f; p != lastRun; p = p->next.load()) {
                            ++bytes_linkn;
                            int ph = p->hash;
                            const K& pk = p->key;
                            V* pv = p->val.load();
                            if ((ph & len) == 0) {
                                ln = new Node(ph, pk, pv, ln);
                            } else {
                                hn = new Node(ph, pk, pv, hn);
                            }
                        }

                        assert(!(dynamic_cast<ForwardingObject*>(f)));

                        setTabAt(nextTab->tableArray, i, ln);
                        setTabAt(nextTab->tableArray, i + len, hn);
                        setTabAt(tab, i, &*localTable->share);

                        delayDispose.ptr = [=, &keepPin]() -> void { keepPin.retire<RecLinkedNode>(f, lastRun); };
                    } else if (TreeBin* tb = dynamic_cast<TreeBin*>(f)) {
                        TreeBin* t = tb;
                        TreeNode* lo = nullptr;
                        TreeNode* loTail = nullptr;
                        TreeNode* hi = nullptr;
                        TreeNode* hiTail = nullptr;
                        int lc = 0, hc = 0;
                        for (Node* e = t->first; e != nullptr; e = e->next) {
                            int h = e->hash;
                            TreeNode* p = new TreeNode(h, e->key, e->val.load(), nullptr, nullptr);
                            if ((h & len) == 0) {
                                if ((p->prev = loTail) == nullptr)
                                    lo = p;
                                else
                                    loTail->next = p;
                                loTail = p;
                                ++lc;
                            } else {
                                if ((p->prev = hiTail) == nullptr)
                                    hi = p;
                                else
                                    hiTail->next = p;
                                hiTail = p;
                                ++hc;
                            }
                        }
                        bool flag_treebin_lc = false;
                        int num = 0;
                        ;
                        ln = (lc <= UNTREEIFY_THRESHOLD) ? untreeify(lo, num)
                                                         : (hc != 0) ? new TreeBin(lo) : (flag_treebin_lc = true, t);

                        bool flag_treebin_hc = false;
                        hn = (hc <= UNTREEIFY_THRESHOLD) ? untreeify(hi, num)
                                                         : (lc != 0) ? new TreeBin(hi) : (flag_treebin_hc = true, t);

                        long rec_bytes = 0;

                        TreeNode* lo_ptr = nullptr;
                        if (lc <= UNTREEIFY_THRESHOLD || hc == 0) {
                            lo_ptr = lo;
                            rec_bytes += lc * sizeof(TreeNode);
                        }

                        TreeNode* hi_ptr = nullptr;
                        if (hc <= UNTREEIFY_THRESHOLD || lc == 0) {
                            hi_ptr = hi;
                            rec_bytes += hc * sizeof(TreeNode);
                        }

                        TreeBin* tree_bin_ptr = nullptr;
                        if (!flag_treebin_lc && !flag_treebin_hc) {
                            tree_bin_ptr = t;
                            rec_bytes += sizeof(TreeBin) + (lc + hc) * sizeof(TreeNode);
                        }
                        // fwd = new ForwardingObject(nextTab);
                        // fwd = fwdTree;
                        setTabAt(nextTab->tableArray, i, ln);
                        setTabAt(nextTab->tableArray, i + len, hn);
                        setTabAt(tab, i, &*localTable->share);

                        delayDispose.ptr = [=, &keepPin]() -> void {
                            keepPin.retire<RecPartialTree>(tree_bin_ptr, lo_ptr, hi_ptr);
                        };
                    }
                    advance = true;
                }
            }
        }
    }

    static int tableSizeFor(int c) {
        int n = static_cast<unsigned int>(-1) >> numberOfLeadingZeros(c - 1);
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    /**
   * Tries to presize table to accommodate the given number of elements.
   *
   * @param size number of elements (doesn't need to be perfectly accurate)
   */
    void tryPresize(int size, Pin& keepPin) {
        int c = (size >= (int)(static_cast<unsigned int>(MAXIMUM_CAPACITY) >> 1))
                        ? MAXIMUM_CAPACITY
                        : tableSizeFor(size + (static_cast<unsigned int>(size) >> 1) + 1);
        int sc;
        while ((sc = sizeCtl.load()) >= 0) {
            BucketTable* localTable = table.load();
            int n = localTable->length;

            if (c <= sc || n >= MAXIMUM_CAPACITY)
                break;
            else if (localTable == table.load()) {
                int rs = resizeStamp(n);
                if (sizeCtl.compare_exchange_strong(sc, (rs << RESIZE_STAMP_SHIFT) + 2)) transfer(localTable, keepPin);
            }
        }
    }

    /**
   * Replaces all linked nodes in bin at given index unless table is
   * too small, in which case resizes instead.
   */
    void treeifyBin(BucketTable* localTable, int index, Pin& keepPin) {
        Node* b;
        int n;
        if (localTable != nullptr) {
            if ((n = localTable->length) < MIN_TREEIFY_CAPACITY)
                tryPresize(n << 1, keepPin);
            else if ((b = tabAt(localTable->tableArray, index)) != nullptr && b->hash >= 0) {
                std::lock_guard<std::mutex> control(localTable->lock_levels[index]);
                if (tabAt(localTable->tableArray, index) == b) {
                    TreeNode* hd = nullptr;
                    TreeNode* tl = nullptr;
                    int num = 0;
                    for (Node* e = b; e != nullptr; e = e->next.load()) {
                        ++num;
                        TreeNode* p = new TreeNode(e->hash, e->key, e->val.load(), nullptr, nullptr);
                        if ((p->prev = tl) == nullptr)
                            hd = p;
                        else
                            tl->next.store(p);
                        tl = p;
                    }
                    // TreeBin* tb = new TreeBin(hd);
                    setTabAt(localTable->tableArray, index, new TreeBin(hd));
                    keepPin.retire<RecSomeNode>(b);
                }
            }
        }
    }

    /**
   * Returns a list of non-TreeNodes replacing those in given list.
   */
    static Node* untreeify(Node* b, int& num) {
        Node* hd = nullptr;
        Node* tl = nullptr;
        int nums = 0;
        for (Node* q = b; q != nullptr; q = q->next) {
            ++nums;
            Node* p = new Node(q->hash, q->key, q->val);
            if (tl == nullptr)
                hd = p;
            else
                tl->next = p;
            tl = p;
        }
        num = nums;
        return hd;
    }

    void addCount(long x, int check, Pin& keepPin) {
        // nums(key/value) in map.
        long s = baseCount.fetch_add(x) + x;

        // check for resize.
        if (check >= 0) {
            BucketTable* localTable;
            BucketTable* nt;
            int n, sc;
            while (s >= (long)(sc = sizeCtl.load()) && (n = (localTable = table.load())->length) < MAXIMUM_CAPACITY) {
                int rs = resizeStamp(n) << RESIZE_STAMP_SHIFT;

                if (sc < 0) {
                    if (sc == rs + MAX_RESIZERS || sc == rs + 1 || (nt = nextTable.load()) == nullptr ||
                        transferIndex.load() <= 0)
                        break;
                    if (sizeCtl.compare_exchange_strong(sc, sc + 1)) {
                        transfer(localTable, nt, keepPin);
                    }
                } else if (sizeCtl.compare_exchange_strong(sc, rs + 2)) {
                    transfer(localTable, keepPin);
                }
                s = baseCount.load();
            }
        }
    }

    /**
   * Helps transfer if a resize is in progress.
   */
    BucketTable* helpTransfer(BucketTable* localTable, Node* f, Pin& keepPin) {
        BucketTable* nextTab;
        int sc;
        ForwardingObject* ff;
        ff = dynamic_cast<ForwardingObject*>(f);
        nextTab = ff->nextTable;
        int rs = resizeStamp(localTable->length) << RESIZE_STAMP_SHIFT;
        while (nextTab == nextTable.load() && table.load() == localTable && (sc = sizeCtl.load()) < 0) {
            if (sc == rs + MAX_RESIZERS || sc == rs + 1 || transferIndex.load() <= 0) break;
            if (sizeCtl.compare_exchange_strong(sc, sc + 1)) {
                transfer(localTable, nextTab, keepPin);
                break;
            }
        }

        return nextTab;
    }

    std::atomic<BucketTable*> table;
    std::atomic<BucketTable*> nextTable;
    std::atomic<long> baseCount;
    std::atomic<int> sizeCtl;
    std::atomic<int> transferIndex;

public:
    ConcurrentHashMap()
            : ConcurrentBridge<ConcurrentHashMap<K, V>>(),
              table(nullptr),
              nextTable(nullptr),
              baseCount(0),
              sizeCtl(0),
              transferIndex(0) {
        initTable();
    }

    ~ConcurrentHashMap() { delete table.load(); }

    bool empty() { return baseCount.load() == 0; }

    long max_size() { return MAXIMUM_CAPACITY; }

    long size() { return baseCount.load(); }

    class ConstKeyValueIterator {
        friend class ConcurrentHashMap;

    private:
        ConstKeyValueIterator(ConcurrentHashMap* map) : keepPin(map) {}

        void reset() { keepPin.reset(); }

        void set_node(Node* node) { curr = node; }

    public:
        const K& key() { return curr->key; }

        const V& val() { return *curr->val.load(); }

        const bool is_data() { return keepPin.has_value(); }

    private:
        std::optional<Pin> keepPin;
        Node* curr;
    };

    class ConstIterator {
        friend class ConcurrentHashMap;

    private:
        ConstIterator(ConcurrentHashMap* map)
                : keepPin(map), table(map->table.load()), curr(tabAt(table->tableArray, 0)), index(0) {
            while (curr == nullptr && (++index) < table->length) {
                curr = tabAt(table->tableArray, index);
            }

            if (index == table->length) {
                index = -1;
                curr = nullptr;
                table = nullptr;
                return;
            }

            assert(curr != nullptr);
            if (curr->hash == TREEBIN) {
                curr = static_cast<TreeNode*>(static_cast<TreeBin*>(curr)->first);
            }
        }

        ConstIterator() : keepPin(), table(nullptr), curr(nullptr), index(-1) {}

    public:
        ConstIterator& operator++() {
            if (index == -1) {
                return *this;
            }

            Node* next;
            if ((next = curr->next.load()) != nullptr) {
                curr = next;
                return *this;
            }

            for (;;) {
                if ((++index) < table->length) {
                    curr = tabAt(table->tableArray, index);
                    if (curr != nullptr) {
                        if (curr->hash == TREEBIN) {
                            curr = static_cast<TreeNode*>(static_cast<TreeBin*>(curr)->first);
                        }
                        return *this;
                    } else {
                        continue;
                    }
                }
                assert(index == table->length);
                index = -1;
                curr = nullptr;
                table = nullptr;
                return *this;
            }
        }

        bool operator==(const ConstIterator& o) const {
            if (index == -1 && o.index == -1) return true;

            return table == o.table && index == o.index && curr == o.curr;
        }

        bool operator!=(const ConstIterator& o) const { return !(*this == o); }

        const K& key() { return curr->key; }

        const V& val() { return *curr->val.load(); }

    private:
        std::optional<Pin> keepPin;
        const BucketTable* table;
        Node* curr;
        int index;
    };

    ConstIterator begin() { return ConstIterator(this); }

    ConstIterator end() { return ConstIterator(); }

    bool find(const K& key, V* value) {
        BucketTable* localTable;
        std::atomic<Node*>* tab;
        Node* e;
        int n, eh;
        int h = spread(Hash()(key));
        Pin keepPin(this);

        localTable = table.load();
        tab = localTable->tableArray;
        n = localTable->length;
        if ((e = tabAt(tab, (n - 1) & h)) == nullptr) return false;

        if ((eh = e->hash) == h) {
            if (KeyEqual()(e->key, key)) {
                *value = *e->val.load();
                return true;
            }
        } else if (eh < 0) {
            Node* result = e->find(h, key);
            if (result != nullptr) {
                *value = *result->val.load();
                return true;
            }
            return false;
        }

        // wait for treeifyBin...
        while ((e = e->next.load()) != nullptr) {
            if (e->hash == h && KeyEqual()(e->key, key)) {
                *value = *e->val.load();
                return true;
            }
        }

        return false;
    }

    ConstKeyValueIterator find_reference(const K& key) {
        BucketTable* localTable;
        std::atomic<Node*>* tab;
        Node* e;
        int n, eh;
        int h = spread(Hash()(key));
        ConstKeyValueIterator iter(this);

        localTable = table.load();
        tab = localTable->tableArray;
        n = localTable->length;
        if ((e = tabAt(tab, (n - 1) & h)) == nullptr) {
            iter.reset();
            return iter;
        }

        if ((eh = e->hash) == h) {
            if (KeyEqual()(e->key, key)) {
                iter.set_node(e);
                return iter;
            }
        } else if (eh < 0) {
            Node* result = e->find(h, key);
            if (result != nullptr) {
                iter.set_node(result);
                return iter;
            }
            iter.reset();
            return iter;
        }

        // wait for treeifyBin...
        while ((e = e->next.load()) != nullptr) {
            if (e->hash == h && KeyEqual()(e->key, key)) {
                iter.set_node(e);
                return iter;
            }
        }

        iter.reset();
        return iter;
    }

    bool insert(const K& key, V* value) { return insert(key, value, false); }

    bool insertAbsent(const K& key, const V& value) { return insert(key, const_cast<V*>(&value), true); }

    /**
   * Removes the key (and its corresponding value) from this map.
   * This method does nothing if the key is not in the map.
   *
   * @param  key the key that needs to be removed
   * @return the previous value associated with {@code key}, or
   *         {@code null} if there was no mapping for {@code key}
   * @throws NullPointerException if the specified key is null
   */
    bool erase(const K& key, V* value) { return erase(key, value, false); }

    bool eraseEqual(const K& key, const V& value) { return erase(key, const_cast<V*>(&value), true); }

private:
    bool insert(const K& key, V* value, const bool absent) {
        int hash = spread(Hash()(key));
        int binCount = 0;
        std::atomic<Node*>* tab;
        Node* f;
        int n, i, fh;

        Pin keepPin(this);
        BucketTable* localTable = table.load();
        for (;;) {
            n = localTable->length;
            tab = localTable->tableArray;

            if ((f = tabAt(tab, i = (n - 1) & hash)) == nullptr) {
                Node* newNode = new Node(hash, key, *value);
                if (casTabAt(tab, i, f, newNode)) {
                    addCount(1, 0, keepPin);
                    return true;
                }
                newNode->shallow = false;
                delete newNode;
            }

            if ((fh = f->hash) == MOVED) {
                localTable = helpTransfer(localTable, f, keepPin);
                continue;
            }

            if (absent && fh == hash && KeyEqual()(f->key, key)) {
                return false;
            } else {
                DelayDispose delayDispose;
                std::lock_guard<std::mutex> control(localTable->lock_levels[i]);

                // take bucket's head.
                if (f == tabAt(tab, i)) {
                    if (fh >= 0) {
                        binCount = 1;
                        for (Node* e = f;; ++binCount) {
                            if ((e->hash == hash) && KeyEqual()(e->key, key)) {
                                auto old = e->val.load();
                                if (!absent) {
                                    e->val.store(new V(*value));
                                    keepPin.retire<RecSingleNode<V>>(old);
                                }

                                delayDispose.ptr = ([=, &keepPin]() {
                                    if (binCount >= TREEIFY_THRESHOLD) {
                                        treeifyBin(localTable, i, keepPin);
                                    }

                                    if (!absent) {
                                        *value = *old;
                                    }
                                });

                                if (absent) {
                                    return false;
                                } else {
                                    return true;
                                }
                            }

                            Node* pred = e;
                            if ((e = e->next.load()) == nullptr) {
                                pred->next.store(new Node(hash, key, *value));

                                delayDispose.ptr = ([=, &keepPin]() {
                                    if (binCount >= TREEIFY_THRESHOLD) treeifyBin(localTable, i, keepPin);
                                    addCount(1, binCount, keepPin);
                                });
                                return true;
                            }
                        }
                    } else {
                        TreeBin* tb = static_cast<TreeBin*>(f);
                        Node* p;
                        if ((p = tb->putTreeVal(hash, key, *value, keepPin)) != nullptr) {
                            if (!absent) {
                                auto old = p->val.load();
                                p->val.store(new V(*value));
                                keepPin.retire<RecSingleNode<V>>(old);
                                *value = *old;
                            }

                            if (absent) {
                                return false;
                            } else {
                                return true;
                            }
                        } else {
                            delayDispose.ptr = [=, &keepPin]() { addCount(1, 2, keepPin); };

                            return true;
                        }
                    }
                }
            }
        }

        // never use
        return true;
    }

    bool erase(const K& key, V* value, const bool equal) {
        int hash = spread(Hash()(key));
        std::atomic<Node*>* tab;

        Node* f;
        int n, i, fh;

        Pin keepPin(this);
        BucketTable* localTable = table.load();
        for (;;) {
            n = localTable->length;
            tab = localTable->tableArray;
            if ((f = tabAt(tab, i = (n - 1) & hash)) == nullptr) return false;

            if ((fh = f->hash) == MOVED) {
                localTable = helpTransfer(localTable, f, keepPin);
                continue;
            }

            const V* oldVal = nullptr;
            DelayDispose delayDispose;
            std::lock_guard<std::mutex> control(localTable->lock_levels[i]);

            // take bucket's head.
            if (f == tabAt(tab, i)) {
                if (fh >= 0) {
                    Node* pred = nullptr;
                    for (Node* e = f;;) {
                        // const K* ek;
                        if (e->hash == hash && KeyEqual()(e->key, key)) {
                            oldVal = e->val.load();
                            if (!equal || (*oldVal) == (*value)) {
                                if (pred != nullptr) {
                                    pred->next.store(e->next.load());
                                } else {
                                    setTabAt(tab, i, e->next);
                                }

                                delayDispose.ptr = [=, &keepPin]() {
                                    e->shallow = false;
                                    keepPin.retire<RecSingleNode<Node>>(e);
                                    addCount(-1L, -1, keepPin);
                                    if (!equal) {
                                        *value = *oldVal;
                                    }
                                };

                                return true;
                            } else {
                                return false;
                            }
                        }

                        pred = e;
                        if ((e = e->next.load()) == nullptr) {
                            return false;
                        }
                    }
                } else {
                    TreeBin* t = static_cast<TreeBin*>(f);
                    TreeNode* r;
                    TreeNode* p;
                    // int value;
                    if ((r = t->root) != nullptr && (p = r->findTreeNode(hash, key)) != nullptr) {
                        oldVal = p->val.load();
                        if (!equal || (*oldVal) == (*value)) {
                            if (t->removeTreeNode(p, keepPin, delayDispose)) {
                                int num = 0;
                                setTabAt(tab, i, untreeify(t->first, num));

                                auto ptr = delayDispose.ptr;
                                delayDispose.ptr = [=, &keepPin]() {
                                    if (ptr != nullptr) ptr();
                                    keepPin.retire<RecTreeBin>(t);
                                };
                            }

                            auto ptr = delayDispose.ptr;
                            delayDispose.ptr = [=, &keepPin]() -> void {
                                if (ptr != nullptr) ptr();
                                addCount(-1L, -1, keepPin);
                                if (!equal) {
                                    *value = *oldVal;
                                }
                            };

                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }

        // never use
        return false;
    }
};

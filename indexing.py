
from abc import ABC, abstractmethod
from typing import Iterable, Any, Optional


class Position(ABC):
    @abstractmethod
    def get_element(self):
        """Return the element stored at this position."""
        pass
    
    def __eq__(self, other):
        """Return True if other is a Position representing the same location."""
        raise NotImplementedError('must be implemented by subclass')
    
    def __ne__(self, other):
        """Return True if other does not represent the same location."""
        return not (self == other)

class Tree(ABC):
    """Abstract base class representing a tree structure."""
    
    @abstractmethod
    def __len__(self) -> int:
        """Return the total number of elements in the tree."""
        pass

    def is_empty(self) -> bool:
        """Return True if the tree is empty."""
        return len(self) == 0

    @abstractmethod
    def __iter__(self):
        """Generate an iteration of the tree's elements."""
        pass

    @abstractmethod
    def positions(self) -> Iterable[Position]:
        """Generate an iteration of the tree's positions."""
        pass

    @abstractmethod
    def root(self) -> Optional[Position]:
        """Return the root Position of the tree (or None if tree is empty)."""
        pass

    @abstractmethod
    def parent(self, p: Position) -> Optional[Position]:
        """Return the Position of p's parent (or None if p is root)."""
        pass

    @abstractmethod
    def children(self, p: Position) -> Iterable[Position]:
        """Return an iterable collection containing the children of Position p."""
        pass

    @abstractmethod
    def num_children(self, p: Position) -> int:
        """Return the number of children that Position p has."""
        pass

    def is_internal(self, p: Position) -> bool:
        """Return True if Position p has at least one child."""
        return self.num_children(p) > 0

    def is_external(self, p: Position) -> bool:
        """Return True if Position p has no children."""
        return self.num_children(p) == 0

    def is_root(self, p: Position) -> bool:
        """Return True if Position p represents the root of the tree."""
        return p == self.root()

class AbstractTree(Tree):
    """An abstract base class providing some common tree methods."""
    
    def depth(self, p: Position) -> int:
        """Return the number of levels separating Position p from the root."""
        if self.is_root(p):
            return 0
        else:
            return 1 + self.depth(self.parent(p))

class BinaryTree(Tree):
    """Abstract base class representing a binary tree structure."""

    @abstractmethod
    def left(self, p: Position) -> Optional[Position]:
        """Return the Position of p's left child (or None if no child exists)."""
        pass

    @abstractmethod
    def right(self, p: Position) -> Optional[Position]:
        """Return the Position of p's right child (or None if no child exists)."""
        pass
    
    def sibling(self, p: Position) -> Optional[Position]:
        """Return the Position of p's sibling (or None if no sibling exists)."""
        parent = self.parent(p)
        if parent is None:
            return None
        if p == self.left(parent):
            return self.right(parent)
        else:
            return self.left(parent)

class AbstractBinaryTree(BinaryTree, AbstractTree):
    """Abstract base class providing functionality for BinaryTree."""
    
    def num_children(self, p: Position) -> int:
        """Return the number of children of Position p."""
        count = 0
        if self.left(p) is not None:
            count += 1
        if self.right(p) is not None:
            count += 1
        return count

    def children(self, p: Position) -> Iterable[Position]:
        """Generate an iteration of Positions representing p's children."""
        if self.left(p) is not None:
            yield self.left(p)
        if self.right(p) is not None:
            yield self.right(p)

    def inorder(self) -> Iterable[Position]:
        """Generate an inorder iteration of positions (nodes) in the tree."""
        if not self.is_empty():
            for p in self._subtree_inorder(self.root()):
                yield p

    def _subtree_inorder(self, p: Position) -> Iterable[Position]:
        """Generate an inorder iteration of positions of the subtree rooted at p."""
        if self.left(p) is not None:
            yield from self._subtree_inorder(self.left(p))
        
        yield p
        
        if self.right(p) is not None:
            yield from self._subtree_inorder(self.right(p))



class LinkedBinaryTree(AbstractBinaryTree):
    """Concrete implementation of a binary tree using a node-based, linked structure."""

    class _Node(Position):
        """Nested Node class that acts as a Position."""
        __slots__ = '_element', '_parent', '_left', '_right'

        def __init__(self, e, parent=None, left=None, right=None):
            self._element = e
            self._parent = parent
            self._left = left
            self._right = right

        def get_element(self):
            if self._parent is self:  # convention for defunct node
                raise RuntimeError("Position no longer valid")
            return self._element
        
        def get_parent(self): return self._parent
        def get_left(self): return self._left
        def get_right(self): return self._right
        def set_element(self, e): self._element = e
        def set_parent(self, parent): self._parent = parent
        def set_left(self, left): self._left = left
        def set_right(self, right): self._right = right
        
        def __eq__(self, other):
            return type(other) is type(self) and other._element == self._element and other._parent == self._parent # Simplistic equality check

    def __init__(self):
        self._root = None
        self._size = 0

    def _validate(self, p):
        """Validates the position and returns it as a node."""
        if not isinstance(p, self._Node):
            raise RuntimeError("Not valid position type")
        if p.get_parent() is p:
            raise RuntimeError("p is no longer in the tree")
        return p

    def _make_node(self, e, parent=None, left=None, right=None):
        """Factory function to create a new node storing element e."""
        return self._Node(e, parent, left, right)

    def __len__(self) -> int: return self._size
    def root(self) -> Optional[Position]: return self._root
    def parent(self, p: Position) -> Optional[Position]: return self._validate(p).get_parent()
    def left(self, p: Position) -> Optional[Position]: return self._validate(p).get_left()
    def right(self, p: Position) -> Optional[Position]: return self._validate(p).get_right()
    
    def __iter__(self) -> Iterable[Any]:
        """Generate an iteration of the tree's elements in inorder."""
        for p in self.inorder():
            yield p.get_element()

    def positions(self) -> Iterable[Position]:
        """Generate an iteration of the tree's positions (using inorder traversal)."""
        yield from self.inorder()

    def add_root(self, e):
        if self._root is not None: raise RuntimeError("Tree is not empty")
        self._root = self._make_node(e, None, None, None)
        self._size = 1
        return self._root

    def add_left(self, p, e):
        parent = self._validate(p)
        if parent.get_left() is not None: raise RuntimeError("p already has a left child")
        child = self._make_node(e, parent, None, None)
        parent.set_left(child)
        self._size += 1
        return child

    def add_right(self, p, e):
        parent = self._validate(p)
        if parent.get_right() is not None: raise RuntimeError("p already has a right child")
        child = self._make_node(e, parent, None, None)
        parent.set_right(child)
        self._size += 1
        return child
        
    def set(self, p, e):
        """Replaces the element at Position p with e and returns the replaced element."""
        node = self._validate(p)
        temp = node.get_element()
        node.set_element(e)
        return temp

    def remove(self, p):
        """Removes the node at Position p and replaces it with its child, if any."""
        node = self._validate(p)
        if self.num_children(p) == 2:
            raise RuntimeError("p has two children")
        child = node.get_left() if node.get_left() is not None else node.get_right()
        if child is not None:
            child.set_parent(node.get_parent())
        if node == self._root:
            self._root = child
        else:
            parent = node.get_parent()
            if node == parent.get_left():
                parent.set_left(child)
            else:
                parent.set_right(child)
        self._size -= 1
        temp = node.get_element()
        node.set_element(None)
        node.set_parent(node)  # convention for defunct node
        return temp



class Map(ABC):

    class _MapEntry:
        """Lightweight composite to store key-value pairs."""
        def __init__(self, key, value):
            self._key = key
            self._value = value
        
        def get_key(self): return self._key
        def get_value(self): return self._value

        def __lt__(self, other): return self._key < other.get_key()
        def __le__(self, other): return self._key <= other.get_key()
        def __eq__(self, other): return self._key == other.get_key()
        def __repr__(self): return f"({self._key}, {self._value})"


class BalanceableBinaryTree(LinkedBinaryTree):
    """A specialized version of LinkedBinaryTree with support for balancing."""

    class _BSTNode(LinkedBinaryTree._Node):
        """Node storing an auxiliary integer field (height)."""
        __slots__ = LinkedBinaryTree._Node.__slots__ + ('_aux',)

        def __init__(self, e, parent=None, left=None, right=None):
            super().__init__(e, parent, left, right)
            self._aux = 0  # Used for height/balance factor

        def get_aux(self): return self._aux
        def set_aux(self, value): self._aux = value

    def _make_node(self, e, parent=None, left=None, right=None):
        return self._BSTNode(e, parent, left, right)

    def _get_height(self, p: Optional[Position]) -> int:
        """Return the height of position p (or 0 if None)."""
        if p is None: return 0
        return self._validate(p).get_aux()

    def _update_height(self, p: Position) -> None:
        """Compute and update the height of position p."""
        node = self._validate(p)
        h_left = self._get_height(node.get_left())
        h_right = self._get_height(node.get_right())
        node.set_aux(1 + max(h_left, h_right))

    def _is_balanced(self, p: Position) -> bool:
        """Return True if Position p satisfies the AVL property."""
        h_left = self._get_height(self.left(p))
        h_right = self._get_height(self.right(p))
        return abs(h_left - h_right) <= 1

    def _taller_child(self, p: Position) -> Optional[Position]:
        """Return the taller child of p."""
        h_left = self._get_height(self.left(p))
        h_right = self._get_height(self.right(p))
        if h_left > h_right:
            return self.left(p)
        elif h_right > h_left:
            return self.right(p)
        else:
            return self.left(p) # Tie-breaker: prefer left

    def _relink(self, parent, child, make_left_child):
        """Relink a parent node with its oriented child node."""
        if child is not None:
            child.set_parent(parent)
        if make_left_child:
            parent.set_left(child)
        else:
            parent.set_right(child)

    def rotate(self, p: Position):
        x = self._validate(p)
        y = x.get_parent()
        z = y.get_parent()

        if z is None:
            self._root = x
            x.set_parent(None)
        else:
            self._relink(z, x, y == z.get_left())

        if x == y.get_left():
            self._relink(y, x.get_right(), True)
            self._relink(x, y, False)
        else:
            self._relink(y, x.get_left(), False)
            self._relink(x, y, True)

    def restructure(self, x: Position) -> Position:
        y = self.parent(x)
        z = self.parent(y)

        if (self.left(z) == y) == (self.left(y) == x):
            self.rotate(y)
            return y
        else:
            self.rotate(x)
            self.rotate(x)
            return x
            
    def _rebalance_avl(self, p: Optional[Position]) -> None:
        """Rebalance node p after insertion or deletion."""
        while p is not None:
            old_height = self._get_height(p)
            self._update_height(p)
            
            if not self._is_balanced(p):
                y = self._taller_child(p)
                x = self._taller_child(y)
                p = self.restructure(x)
                
                self._update_height(self.left(p))
                self._update_height(self.right(p))
                self._update_height(p)
            
            if self._get_height(p) == old_height:
                break

            p = self.parent(p)


class AVLTreeMap:
    """Map implementation using a Balanceable Binary Search Tree (AVL)."""

    def __init__(self):
        self._tree = BalanceableBinaryTree()
        self._size = 0

    def __len__(self) -> int:
        return self._size

    def __iter__(self) -> Iterable[Any]:
        """Generate an iteration of the map's keys in order."""
        for entry in self._tree:
            yield entry.get_key()

    def values(self) -> Iterable[Any]:
        """Generate an iteration of the map's values in order."""
        for entry in self._tree:
            yield entry.get_value()

    def _tree_search(self, p: Optional[Position], k: Any) -> Optional[Position]:
        """Return Position with key k, or None."""
        if p is None:
            return None
        
        entry = p.get_element()
        if k == entry.get_key():
            return p
        elif k < entry.get_key():
            return self._tree_search(self._tree.left(p), k)
        else:
            return self._tree_search(self._tree.right(p), k)

    def _subtree_first_position(self, p: Position) -> Position:
        """Return Position of first item in subtree p."""
        walk = p
        while self._tree.left(walk) is not None:
            walk = self._tree.left(walk)
        return walk

    def _find_position(self, k: Any) -> tuple[Optional[Position], Optional[Position]]:
        """Finds Position of key k, or the last Position visited (potential parent)."""
        if self._tree.is_empty():
            return None, None
        
        walk = self._tree.root()
        parent = None
        while walk is not None:
            entry = walk.get_element()
            if k == entry.get_key():
                return walk, parent  # Found
            elif k < entry.get_key():
                parent = walk
                walk = self._tree.left(walk)
            else:
                parent = walk
                walk = self._tree.right(walk)
        return None, parent # Not found, return potential parent

    def get(self, k: Any) -> Optional[Any]:
        """Return the value associated with key k, or None."""
        p, _ = self._find_position(k)
        if p is None:
            return None
        return p.get_element().get_value()

    def put(self, k: Any, v: Any) -> Optional[Any]:
        """Insert or replace entry (k, v) and return old value, or None."""
        entry = Map._MapEntry(k, v)
        old_value = None

        if self._tree.is_empty():
            p = self._tree.add_root(entry)
            self._size += 1
        else:
            p, parent = self._find_position(k)
            
            if p is not None:  # Key found, replace value
                old_entry = self._tree.set(p, entry)
                return old_entry.get_value()
            else: # Key not found, insert
                if k < parent.get_element().get_key():
                    p = self._tree.add_left(parent, entry)
                else:
                    p = self._tree.add_right(parent, entry)
                self._size += 1

        self._tree._rebalance_avl(p) # Rebalance after insertion
        return old_value

    def remove(self, k: Any) -> Optional[Any]:
        """Remove entry with key k and return its value, or None."""
        p, _ = self._find_position(k)
        if p is None:
            return None

        if self._tree.num_children(p) == 2:
            successor = self._subtree_first_position(self._tree.right(p))
            p.get_element()._key = successor.get_element().get_key()
            p.get_element()._value = successor.get_element().get_value()
            p = successor # Now remove the successor node (which has 0 or 1 child)

        parent = self._tree.parent(p)
        old_value = self._tree.remove(p).get_value()
        self._size -= 1
        
        self._tree._rebalance_avl(parent) # Rebalance up from the removal point's parent
        return old_value

    def sub_map(self, k1: Any, k2: Any) -> Iterable[Any]:
        """Generate values for keys k such that k1 <= k < k2."""
        
        walk = self._tree.root()
        current = None
        
        while walk is not None:
            key = walk.get_element().get_key()
            if k1 <= key:
                current = walk
                walk = self._tree.left(walk)
            else:
                walk = self._tree.right(walk)

        while current is not None:
            key = current.get_element().get_key()
            if key < k2:
                yield current.get_element().get_value()
                
                if self._tree.right(current) is not None:
                    current = self._subtree_first_position(self._tree.right(current))
                else:
                    parent = self._tree.parent(current)
                    while parent is not None and current == self._tree.right(parent):
                        current = parent
                        parent = self._tree.parent(current)
                    current = parent
            else:
                break

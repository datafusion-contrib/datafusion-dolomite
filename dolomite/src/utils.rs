/// A utility builder for tree like data structures.
///
/// See [`OptExpression`] and [`Pattern`].
pub trait TreeBuilder: Sized {
    type Node;
    type Tree: From<(Self::Node, Vec<Self::Tree>)>;
    type Output;

    fn begin<K: Into<Self::Node>>(
        self,
        node: K,
    ) -> NonRootBuilder<Self::Tree, Self::Node, Self> {
        self.begin_node(node.into())
    }

    fn begin_node(
        self,
        node: Self::Node,
    ) -> NonRootBuilder<Self::Tree, Self::Node, Self> {
        NonRootBuilder {
            parent: self,
            node,
            children: vec![],
        }
    }

    fn leaf<K: Into<Self::Node>>(self, node: K) -> Self {
        self.leaf_node(node.into())
    }

    fn leaf_node(self, node: Self::Node) -> Self {
        let tree = Self::Tree::from((node, vec![]));
        self.add_child(tree)
    }

    fn end(self) -> Self::Output;

    fn add_child(self, tree: Self::Tree) -> Self;
}

pub struct RootBuilder<T, N> {
    node: N,
    children: Vec<T>,
}

impl<T, N> RootBuilder<T, N> {
    pub fn new(node: N) -> Self {
        Self {
            node,
            children: vec![],
        }
    }
}

pub struct NonRootBuilder<T, N, P> {
    parent: P,
    node: N,
    children: Vec<T>,
}

impl<T, N> TreeBuilder for RootBuilder<T, N>
where
    T: From<(N, Vec<T>)>,
{
    type Node = N;
    type Tree = T;
    type Output = T;

    fn end(self) -> T {
        T::from((self.node, self.children))
    }

    fn add_child(mut self, tree: T) -> Self {
        self.children.push(tree);
        self
    }
}

impl<T, N, P> TreeBuilder for NonRootBuilder<T, N, P>
where
    T: From<(N, Vec<T>)>,
    P: TreeBuilder<Node = N, Tree = T>,
{
    type Node = N;
    type Tree = T;
    type Output = P;

    fn end(self) -> P {
        let tree = T::from((self.node, self.children));
        self.parent.add_child(tree)
    }

    fn add_child(mut self, tree: T) -> Self {
        self.children.push(tree);
        self
    }
}

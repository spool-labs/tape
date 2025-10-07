/// Return the first global index at a given layer for a perfect binary Merkle tree of `height`.
/// Layers are numbered with leaves at 0 and root at `height`.
pub fn first_index_in_layer(layer: usize, height: usize) -> usize {
    if layer == 0 {
        0
    } else {
        (1usize << (height + 1)) - (1usize << (height + 1 - layer))
    }
}

/// Return the ancestor index of `node_index` at absolute `target_layer`
/// for a perfect binary Merkle tree of `height`.
/// Layers are numbered with leaves at 0 and root at `height`.
///
/// Example (height = 3):
///   - find_ancestor(2, 3, 3)  == 12
///   - find_ancestor(2, 10, 3) == 13
pub fn find_ancestor(target_layer: usize, node_index: usize, height: usize) -> usize {
    assert!(target_layer <= height, "target_layer exceeds tree height");

    // Determine the layer of `node_index`.
    // Find the largest L such that first_index_in_layer(L) <= node_index.
    let mut src_layer = 0usize;
    while src_layer < height && node_index >= first_index_in_layer(src_layer + 1, height) {
        src_layer += 1;
    }

    assert!(
        target_layer >= src_layer,
        "target_layer must be >= the node's current layer (ancestor lookup)"
    );

    // Position within its source layer, then shift right by the number of layers we go up.
    let pos_in_src = node_index - first_index_in_layer(src_layer, height);
    let up = target_layer - src_layer;

    first_index_in_layer(target_layer, height) + (pos_in_src >> up)
}

/// Return the (start, count) range of descendant indices of `node_index`
/// located at `target_layer` (leaves = 0, root = `height`) in a perfect
/// binary Merkle tree indexed by layer left→right as in the prompt.
///
/// Same-layer query returns (node_index, 1).
pub fn descendant_range(node_index: usize, target_layer: usize, height: usize) -> (usize, usize) {
    assert!(target_layer <= height, "target_layer exceeds tree height");

    // Max valid index for a perfect tree with given height.
    let last_index = (1usize << (height + 1)) - 2;
    assert!(
        node_index <= last_index,
        "node_index out of range for given height"
    );

    // Determine the layer of `node_index`.
    let mut src_layer = 0usize;
    while src_layer < height && node_index >= first_index_in_layer(src_layer + 1, height) {
        src_layer += 1;
    }

    assert!(
        target_layer <= src_layer,
        "target_layer must be <= the node's current layer (descendant lookup)"
    );

    // Position within source layer; expand down by `down` layers.
    let pos_in_src = node_index - first_index_in_layer(src_layer, height);
    let down = src_layer - target_layer;
    let count = 1usize << down;
    let start = first_index_in_layer(target_layer, height) + (pos_in_src << down);

    (start, count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand((start, count): (usize, usize)) -> Vec<usize> {
        (start..start + count).collect()
    }

    #[test]
    fn test_ancestor() {
        // Leaves 0..7 -> layer 1 parents 8..11
        assert_eq!(find_ancestor(1, 0, 3), 8);
        assert_eq!(find_ancestor(1, 1, 3), 8);

        assert_eq!(find_ancestor(1, 2, 3), 9);
        assert_eq!(find_ancestor(1, 3, 3), 9);

        assert_eq!(find_ancestor(1, 4, 3), 10);
        assert_eq!(find_ancestor(1, 5, 3), 10);

        assert_eq!(find_ancestor(1, 6, 3), 11);
        assert_eq!(find_ancestor(1, 7, 3), 11);

        // Layer 1 nodes 8..11 -> layer 2 parents 12..13
        assert_eq!(find_ancestor(2, 8, 3), 12);
        assert_eq!(find_ancestor(2, 9, 3), 12);
        assert_eq!(find_ancestor(2, 10, 3), 13);
        assert_eq!(find_ancestor(2, 11, 3), 13);

        // Root index for height=3 is 14
        for idx in 0..=14 {
            assert_eq!(find_ancestor(3, idx, 3), 14);
        }

        // Asking for the same layer should return the same index.
        assert_eq!(find_ancestor(0, 5, 3), 5);
        assert_eq!(find_ancestor(1, 8, 3), 8);
        assert_eq!(find_ancestor(2, 13, 3), 13);
        assert_eq!(find_ancestor(3, 14, 3), 14);
    }

    #[test]
    fn test_descendant() {
        // Node 12 (layer 2, left) -> leaves 0..4 and layer1 nodes 8..10
        assert_eq!(descendant_range(12, 0, 3), (0, 4));
        assert_eq!(expand(descendant_range(12, 0, 3)), (0..4).collect::<Vec<_>>());
        assert_eq!(descendant_range(12, 1, 3), (8, 2));
        assert_eq!(descendant_range(12, 2, 3), (12, 1)); // same-layer

        // Node 13 (layer 2, right) -> leaves 4..8 and layer1 nodes 10..12
        assert_eq!(descendant_range(13, 0, 3), (4, 4));
        assert_eq!(expand(descendant_range(13, 0, 3)), (4..8).collect::<Vec<_>>());
        assert_eq!(descendant_range(13, 1, 3), (10, 2));
        assert_eq!(descendant_range(13, 2, 3), (13, 1)); // same-layer
                                                         //
        // Node 9 (layer 1, 'j') -> leaves [2,3]
        assert_eq!(descendant_range(9, 0, 3), (2, 2));
        assert_eq!(expand(descendant_range(9, 0, 3)), vec![2, 3]);
        assert_eq!(descendant_range(9, 1, 3), (9, 1)); // same-layer
    }

    #[test]
    #[should_panic(expected = "target_layer exceeds tree height")]
    fn test_ancestor_panic() {
        let _ = find_ancestor(4, 0, 3);
    }
}

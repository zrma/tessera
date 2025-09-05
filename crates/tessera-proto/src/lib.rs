pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

    #[test]
    fn add_commutative() {
        assert_eq!(add(1, 2), add(2, 1));
        assert_eq!(add(0, 5), add(5, 0));
    }
}

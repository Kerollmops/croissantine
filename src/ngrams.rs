use std::iter::{once, Chain, Once};
use std::str::Chars;

pub struct NGrams3<'a> {
    chars0: Chain<Chain<Once<char>, Chars<'a>>, Once<char>>,
    chars1: Chain<Chain<Once<char>, Chars<'a>>, Once<char>>,
    chars2: Chain<Chain<Once<char>, Chars<'a>>, Once<char>>,
}

impl<'a> NGrams3<'a> {
    pub fn new(text: &'a str) -> Self {
        let chars = once('\x00').chain(text.chars()).chain(once('\x00'));
        let chars0 = chars.clone();
        let mut chars1 = chars.clone();
        let mut chars2 = chars.clone();
        chars1.next();
        chars2.nth(1);
        NGrams3 { chars0, chars1, chars2 }
    }
}

impl Iterator for NGrams3<'_> {
    type Item = [char; 3];

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.chars0.next()?;
        let b = self.chars1.next()?;
        let c = self.chars2.next()?;
        Some([a, b, c])
    }
}

#[cfg(test)]
mod test {
    use super::NGrams3;

    #[test]
    fn normal() {
        let mut iter = NGrams3::new("welcome!");
        assert_eq!(iter.next(), Some(['\x00', 'w', 'e']));
        assert_eq!(iter.next(), Some(['w', 'e', 'l']));
        assert_eq!(iter.next(), Some(['e', 'l', 'c']));
        assert_eq!(iter.next(), Some(['l', 'c', 'o']));
        assert_eq!(iter.next(), Some(['c', 'o', 'm']));
        assert_eq!(iter.next(), Some(['o', 'm', 'e']));
        assert_eq!(iter.next(), Some(['m', 'e', '!']));
        assert_eq!(iter.next(), Some(['e', '!', '\x00']));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn small() {
        let mut iter = NGrams3::new("x");
        assert_eq!(iter.next(), Some(['\x00', 'x', '\x00']));
        assert_eq!(iter.next(), None);
    }
}

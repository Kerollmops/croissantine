use std::iter::Peekable;

pub struct ShrinkContiguousWhitespaces<C: Iterator> {
    chars: Peekable<C>,
    /// Was the previously extracted character a space?
    is_previous_space: bool,
}

impl<C: Iterator<Item = char>> ShrinkContiguousWhitespaces<C> {
    fn new(chars: C) -> Self {
        ShrinkContiguousWhitespaces { chars: chars.peekable(), is_previous_space: false }
    }
}

impl<C: Iterator<Item = char>> Iterator for ShrinkContiguousWhitespaces<C> {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.chars.peek() {
                Some(&c) => {
                    if c.is_whitespace() {
                        self.is_previous_space = true;
                        self.chars.next(); // throw it away
                    } else if self.is_previous_space {
                        self.is_previous_space = false;
                        return Some(' ');
                    } else {
                        return self.chars.next();
                    }
                }
                None => {
                    if self.is_previous_space {
                        self.is_previous_space = false;
                        return Some(' ');
                    } else {
                        return None;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut iter = ShrinkContiguousWhitespaces::new("hel\n\t lo world".chars());
        assert_eq!(iter.next(), Some('h'));
        assert_eq!(iter.next(), Some('e'));
        assert_eq!(iter.next(), Some('l'));
        assert_eq!(iter.next(), Some(' '));
        assert_eq!(iter.next(), Some('l'));
        assert_eq!(iter.next(), Some('o'));
        assert_eq!(iter.next(), Some(' '));
        assert_eq!(iter.next(), Some('w'));
        assert_eq!(iter.next(), Some('o'));
        assert_eq!(iter.next(), Some('r'));
        assert_eq!(iter.next(), Some('l'));
        assert_eq!(iter.next(), Some('d'));
        assert_eq!(iter.next(), None);
    }
}

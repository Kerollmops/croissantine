use std::iter::Peekable;

pub struct ShrinkWhitespaces<C: Iterator> {
    chars: Peekable<C>,
    /// Was the previously extracted character a space?
    is_previous_space: bool,
}

impl<C: Iterator<Item = char>> ShrinkWhitespaces<C> {
    pub fn new(chars: C) -> Self {
        ShrinkWhitespaces { chars: chars.peekable(), is_previous_space: false }
    }
}

impl<C: Iterator<Item = char>> Iterator for ShrinkWhitespaces<C> {
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

impl<C> Clone for ShrinkWhitespaces<C>
where
    C: Iterator + Clone,
    <C as Iterator>::Item: Clone,
{
    fn clone(&self) -> Self {
        ShrinkWhitespaces { chars: self.chars.clone(), is_previous_space: self.is_previous_space }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut iter = ShrinkWhitespaces::new("hel\n\t lo world".chars());
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

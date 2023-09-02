use self::diacritics::RemoveDiacritics;
use self::whitespace::ShrinkWhitespaces;

pub mod diacritics;
pub mod trigrams;
pub mod whitespace;

/// Removes diacritics then shrink whitespaces and then lowercase the characters.
pub fn cleanup_chars<I: Iterator<Item = char> + Clone>(
    chars: I,
) -> impl Iterator<Item = char> + Clone {
    ShrinkWhitespaces::new(RemoveDiacritics::new(chars).flatten()).flat_map(|c| c.to_lowercase())
}

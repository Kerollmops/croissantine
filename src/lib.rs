pub mod available_docids_iter;
pub mod database;
pub mod text;
pub mod treemap_codec;

pub fn encode_trigram(string: &mut String, chars: [char; 3]) -> &str {
    string.clear();
    string.extend(chars);
    string.as_str()
}

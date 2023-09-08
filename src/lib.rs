pub mod available_docids_iter;
pub mod database;
pub mod task;
pub mod text;
pub mod treemap_codec;

pub const DATABASE_MAX_SIZE: usize = 900 * 1024 * 1024 * 1024; // 900 GiB

pub fn encode_trigram(string: &mut String, chars: [char; 3]) -> &str {
    string.clear();
    string.extend(chars);
    string.as_str()
}

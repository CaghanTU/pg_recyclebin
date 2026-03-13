/// Safely quote a PostgreSQL identifier by wrapping in double-quotes and
/// escaping any internal double-quotes (per SQL standard §5.3.6).
#[inline]
pub fn qi(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

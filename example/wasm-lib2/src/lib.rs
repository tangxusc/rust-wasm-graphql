#[allow(warnings)]
mod bindings;

use bindings::exports::example::strings::text::Guest;

struct Component;

impl Guest for Component {
    fn reverse(input: String) -> String {
        input.chars().rev().collect()
    }

    fn char_count(input: String) -> u32 {
        input.chars().count() as u32
    }

    fn repeat(input: String, times: u32) -> String {
        input.repeat(times as usize)
    }
}

bindings::export!(Component with_types_in bindings);

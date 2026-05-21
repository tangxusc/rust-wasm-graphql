mod bindings {
    wit_bindgen::generate!({
        world: "calculator",
        path: "../wit",
    });
}

use bindings::exports::example::calculator::compute::Guest;

struct Component;

impl Guest for Component {
    fn add(a: i32, b: i32) -> i32 {
        a + b
    }

    fn fibonacci(n: i32) -> i64 {
        if n <= 0 {
            return 0;
        }
        if n == 1 {
            return 1;
        }
        let mut a: i64 = 0;
        let mut b: i64 = 1;
        for _ in 2..=n {
            let temp = a + b;
            a = b;
            b = temp;
        }
        b
    }

    fn to_uppercase(input: String) -> String {
        input.to_uppercase()
    }
}

bindings::export!(Component with_types_in bindings);


pub fn to_lower_case_by_sep(input: &str, sep: char) -> String {
    let mut output = String::with_capacity(input.len());

    enum Status {
        PrevSep,
        PrevLower,
        PrevUpper,
        RepeatUpper,
    }

    let mut chars = input.chars();
    let mut status = Status::PrevLower;

    //Specialized handling for adjusting initial state to be correct
    if let Some(ch) = chars.next() {
        if ch.is_uppercase() {
            status = Status::PrevUpper;
            for ch in ch.to_lowercase() {
                output.push(ch);
            }
        } else {
            output.push(ch);
        }
    }

    for ch in chars {
        if ch.is_uppercase() {
            if let Status::PrevLower = status {
                //If previous character is already separator for some reason then don't output it
                output.push(sep);
            }
            for ch in ch.to_lowercase() {
                output.push(ch);
            }
            status = match status {
                Status::PrevLower | Status::PrevSep => Status::PrevUpper,
                Status::PrevUpper | Status::RepeatUpper => Status::RepeatUpper,
            }
        } else {
            if let Status::RepeatUpper = status {
                if let Some(last_ch) = output.pop() {
                    output.push(sep);
                    output.push(last_ch);
                }
            }

            output.push(ch);
            status = if ch == sep {
                Status::PrevSep
            } else {
                Status::PrevLower
            }
        }
    }

    output
}

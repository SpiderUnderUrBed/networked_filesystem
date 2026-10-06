use multipeek::{IteratorExt, MultiPeek};

// TODO: see if I could for the subsequence matching
// handle partial matches at the end of the iterator, signify that and start a remainder
// waiting for the next amount of bytes from a read
// useful with Multipeek potentially for look aheads

pub enum SubsequenceStatus {
    Pending,
    NotMatched,
    Continue,
    FoundAt(usize),
}

// TODO:?
// make it so it will return a pos at continue, it will 100% update starting_offset to it, depending on the situation it will consume the next byte, and continue the loop
pub fn find_subsequence_by_windows_iter(
    delimiter_iter: std::slice::Iter<u8>,
    iter: MultiPeek<std::vec::IntoIter<u8>>,
) -> SubsequenceStatus {
    let remaining_delimiter: Vec<u8> = delimiter_iter.copied().collect();
    let remaining_bytes = iter.collect::<Vec<u8>>();
    match remaining_bytes
        .windows(remaining_delimiter.len())
        .position(|window| window == remaining_delimiter)
    {
        Some(pos) => SubsequenceStatus::FoundAt(pos),
        None => SubsequenceStatus::NotMatched,
    }
}

pub fn find_subsequence_bytes_full(
    delimiter: Vec<u8>,
    bytes: Vec<u8>,
) -> Result<u16, SubsequenceStatus> {
    let mut bytes_iter = bytes.clone().into_iter().multipeek();
    let mut final_pos = 0;
    'subsequence_match: loop {
        for (i, expected) in delimiter.iter().enumerate() {
            match bytes_iter.peek_nth(i) {
                Some(actual) if actual == expected => {}
                Some(_) => {
                    bytes_iter.next();
                    final_pos += 1;
                    continue 'subsequence_match;
                }
                None => return Err(SubsequenceStatus::NotMatched),
            }
        }
        return Ok(final_pos);
    }
}
pub fn find_subsequence_bytes_iter(
    mut delimiter_iter: std::slice::Iter<u8>,
    mut iter: MultiPeek<std::vec::IntoIter<u8>>,
) -> SubsequenceStatus {
    for (position, expected) in delimiter_iter.enumerate() {
        match iter.peek_nth(position) {
            Some(actual) if actual == expected => {
                continue;
            }
            Some(_) => {
                return SubsequenceStatus::Continue;
            }
            None => {
                return SubsequenceStatus::NotMatched;
            }
        }
    }

    // Every delimiter byte matched
    SubsequenceStatus::Pending
}
pub fn find_subsequence_bytes_full_v1(
    delimiter: Vec<u8>,
    bytes: Vec<u8>,
) -> Result<u16, SubsequenceStatus> {
    let mut bytes_iter = bytes.clone().into_iter().multipeek();
    let mut final_pos = 0;
    'subsequence_match: loop {
        for (i, expected) in delimiter.iter().enumerate() {
            if let Some(future_byte) = bytes_iter.peek_nth(i) {
                if future_byte != expected {
                    bytes_iter.next();
                    final_pos += 1;
                    continue 'subsequence_match;
                }
            } else {
                return Err(SubsequenceStatus::NotMatched);
            }
        }
        return Ok(final_pos);
    }
}

pub fn find_subsequence_bytes_minor_iter(
    current_position: usize,
    mut delimiter_iter: std::slice::Iter<u8>,
    mut iter: MultiPeek<std::vec::IntoIter<u8>>,
) -> SubsequenceStatus {
    let expected = {
        if let Some(expected) = delimiter_iter.next() {
            expected
        } else {
            return SubsequenceStatus::Continue;
        }
    };
    match iter.peek_nth(current_position) {
        Some(actual) if actual == expected => return SubsequenceStatus::Pending,
        Some(_) => return SubsequenceStatus::Continue,
        None => return SubsequenceStatus::NotMatched,
        //}
        //}
        // return Ok(final_pos);
    }
}

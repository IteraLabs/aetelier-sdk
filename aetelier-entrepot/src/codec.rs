use std::io::{Read, Write};

use crate::error::EntrepotError;

pub fn decode_lz4(key: &str, bytes: &[u8]) -> Result<Vec<u8>, EntrepotError> {
    let mut out = Vec::new();
    lz4_flex::frame::FrameDecoder::new(bytes)
        .read_to_end(&mut out)
        .map_err(|e| EntrepotError::Decode {
            key: key.to_string(),
            reason: e.to_string(),
        })?;
    Ok(out)
}

pub fn encode_lz4(bytes: &[u8]) -> Vec<u8> {
    let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
    enc.write_all(bytes).expect("vec sink accepts all writes");
    enc.finish().expect("frame finish on vec sink")
}

pub fn utf8_lines(key: &str, decoded: &[u8]) -> Result<Vec<String>, EntrepotError> {
    let text = std::str::from_utf8(decoded).map_err(|e| EntrepotError::Decode {
        key: key.to_string(),
        reason: format!("not utf-8: {e}"),
    })?;
    Ok(text.lines().map(str::to_string).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lz4_frame_round_trips() {
        let original =
            br#"{"channel":"l2Book","data":{"coin":"SOL","time":1694854800000}}
{"channel":"l2Book","data":{"coin":"SOL","time":1694854800550}}
"#;
        let encoded = encode_lz4(original);
        let decoded = decode_lz4("SOL.lz4", &encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn garbage_input_is_a_decode_error_not_a_panic() {
        let err = decode_lz4("bad.lz4", b"not an lz4 frame").unwrap_err();
        assert!(matches!(err, EntrepotError::Decode { .. }));
    }

    #[test]
    fn lines_split_on_newlines_and_reject_binary() {
        let lines = utf8_lines("k", b"a\nb\nc").unwrap();
        assert_eq!(lines, ["a", "b", "c"]);
        assert!(utf8_lines("k", &[0xff, 0xfe, 0x00]).is_err());
    }
}

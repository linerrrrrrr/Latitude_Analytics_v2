# Round 05 v2 real transfer-package probe

- Probe version: `joinquant_financial_futures_round_05_v2`
- Probe report initialization time: `2026-09-03T23:34:54.394064+08:00`
- User-message receive time: `2026-09-03T15:35:02.068Z` (UTC)
- Local record time: `2026-09-03T23:34:59+08:00` from the received attachment metadata
- Run ID: `5ae3afe3-bae0-42c4-a3bb-4d5a1fe4a2ae`
- Remote report completeness: complete BEGIN/END envelope and parseable JSON
- Remote gate result: passed
- Cross-environment file gate: passed

## Evidence identities

- `raw_output.txt`: 12,711 bytes; SHA-256 `a93c2a80dda33935fd96400080f0c573274a48e32d801942e1a97715676855a6`; byte-identical to the received text attachment.
- `result.json`: SHA-256 `20dc53d6b4c95abf13434f55c3c87e6b956a1cc88f9c17a2f9349aac0d6afd13`; extracted without JSON reserialization from the complete envelope.
- `recovery_source.py.txt`: SHA-256 `21bbc78977b0307f8481f541bf91d443a58bd359400f9d0312791d7f0dce2efb`; exact recovery cell supplied in the completed conversation.
- `probe_source.py.txt`: SHA-256 `87e25508478f4701cb8622cbe4671b5901c2ee38978a8f6d7f5eabe953173ae1`; effective v2 source reconstructed by applying the recovery cell's exact, count-checked substitutions to the archived v1 source.
- `downloaded_file_identity.txt`: SHA-256 `296db914d05a0ef855abf57274f1f74b16c287833a9530ccdab422e568301484`; records the downloaded ZIP identity and local member verification.
- The downloaded ZIP itself remains at the user-supplied path and is not copied into the experiment record, so the probe payload is not promoted to project market data.

## Confirmed remotely

- All three bounded request blocks completed and were observation-eligible.
- Daily coverage is exactly one `IF2409.CCFX / 2024-06-28` key.
- Minute session 1 covers exactly 120 keys from 09:31 through 11:30.
- Minute session 2 covers exactly 120 keys from 13:01 through 15:00.
- All returned fields contain zero nulls, zero duplicate keys, zero extra keys, and zero missing keys.
- The package contains one daily record, 240 minute records, and three request-block records.
- JoinQuant built and reread the ZIP successfully, then atomically replaced the old 13,546-byte Round 04 file with an 8,437-byte file whose reported SHA-256 is `e331a7da334a9f62ab5e2708a0675190dcdba84d51dd5c56b0af45081e7516c4`.

## Local download verification

- The supplied file at `D:\Downloads\JQ_FINANCIAL_FUTURES_TRANSFER_PROBE.zip` is exactly 8,437 bytes with SHA-256 `e331a7da334a9f62ab5e2708a0675190dcdba84d51dd5c56b0af45081e7516c4`, identical to the JoinQuant report.
- `zipfile.testzip()` returned `None`; member names, order, compression methods, compressed and uncompressed sizes, and CRC values all match the remote report.
- The disk manifest equals the reported manifest. Every payload byte count, SHA-256, and JSONL record count matches its manifest descriptor.
- The three disk request blocks equal the reported request observations. All are successful, complete, observation-eligible, and passed.
- The daily payload has the exact one expected business key. The minute payload has the exact 240 ordered `+08:00` keys, correct session and request ownership, no duplicate business key, no null business value, and finite numeric field values.

## Gate decision

Round 05 v2 passes. Together with Rounds 01 through 04, all five required empirical rounds are closed, so checklist item 04 may be marked complete. The member names and fields used here remain a probe protocol, not the formal transfer Schema or production filename; those are checklist item 05 decisions.

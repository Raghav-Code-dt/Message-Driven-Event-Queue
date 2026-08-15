/**
 * protocol.js
 * Binary frame encode/decode for the C++ broker wire protocol.
 *
 * Frame layout (17-byte header + payload):
 *  [4B magic BE][1B type][4B payload_len BE][8B msg_id BE]
 *  [2B topic_len BE][topic bytes][body bytes]
 */

export const MAGIC       = 0xDEADBEEF;
export const HEADER_SIZE = 17;

export const MsgType = Object.freeze({
  PUBLISH:    0x01,
  SUBSCRIBE:  0x02,
  ACK:        0x03,
  NACK:       0x04,
  HEARTBEAT:  0x05,
  EVENT_DATA: 0x06,
  ERROR_RESP: 0x07,
});

// ── Header decode ─────────────────────────────────────────────────────────────

/**
 * Decode the 17-byte header from a Buffer.
 * @param {Buffer} buf  Must have at least HEADER_SIZE bytes.
 * @returns {{ valid, type, payloadLen, msgId }}
 */
export function decodeHeader(buf) {
  const magic = buf.readUInt32BE(0);
  return {
    valid:      magic === MAGIC,
    type:       buf.readUInt8(4),
    payloadLen: buf.readUInt32BE(5),
    msgId:      buf.readBigUInt64BE(9),  // BigInt
  };
}

// ── Payload decode ────────────────────────────────────────────────────────────

/** Decode a PUBLISH or EVENT_DATA payload → { topic, body } */
export function decodeEventPayload(payload) {
  if (payload.length < 2) return { topic: '', body: Buffer.alloc(0) };
  const topicLen = payload.readUInt16BE(0);
  const topic    = payload.toString('utf8', 2, 2 + topicLen);
  const body     = payload.slice(2 + topicLen);
  return { topic, body };
}

// ── Frame encoding helpers ────────────────────────────────────────────────────

function writeHeader(buf, type, payloadLen, msgId) {
  let off = 0;
  buf.writeUInt32BE(MAGIC, off);          off += 4;
  buf.writeUInt8(type, off);              off += 1;
  buf.writeUInt32BE(payloadLen, off);     off += 4;
  buf.writeBigUInt64BE(BigInt(msgId), off);
}

/**
 * Encode a PUBLISH or EVENT_DATA frame.
 * @param {number} type      MsgType.PUBLISH or MsgType.EVENT_DATA
 * @param {string} topic
 * @param {Buffer|object} body  Buffer of raw bytes, or a JS object (auto-serialized to JSON)
 * @param {bigint|number} msgId
 */
export function encodeDataFrame(type, topic, body, msgId = 0n) {
  const topicBuf = Buffer.from(topic, 'utf8');
  const bodyBuf  = Buffer.isBuffer(body)
    ? body
    : Buffer.from(JSON.stringify(body), 'utf8');

  const payloadLen = 2 + topicBuf.length + bodyBuf.length;
  const frame      = Buffer.allocUnsafe(HEADER_SIZE + payloadLen);

  writeHeader(frame, type, payloadLen, msgId);

  let off = HEADER_SIZE;
  frame.writeUInt16BE(topicBuf.length, off); off += 2;
  topicBuf.copy(frame, off);                 off += topicBuf.length;
  bodyBuf.copy(frame, off);

  return frame;
}

/**
 * Encode a SUBSCRIBE frame.
 * Payload: [2B topic_len][topic][2B group_len][group]
 */
export function encodeSubscribe(topic, group = '', msgId = 0n) {
  const topicBuf = Buffer.from(topic, 'utf8');
  const groupBuf = Buffer.from(group, 'utf8');

  const payloadLen = 2 + topicBuf.length + 2 + groupBuf.length;
  const frame      = Buffer.allocUnsafe(HEADER_SIZE + payloadLen);

  writeHeader(frame, MsgType.SUBSCRIBE, payloadLen, msgId);

  let off = HEADER_SIZE;
  frame.writeUInt16BE(topicBuf.length, off); off += 2;
  topicBuf.copy(frame, off);                 off += topicBuf.length;
  frame.writeUInt16BE(groupBuf.length, off); off += 2;
  groupBuf.copy(frame, off);

  return frame;
}

/**
 * Encode an ACK or NACK frame (no payload — msg_id is in the header).
 */
export function encodeAck(type, msgId) {
  const frame = Buffer.allocUnsafe(HEADER_SIZE);
  writeHeader(frame, type, 0, BigInt(msgId));
  return frame;
}

/**
 * Encode a HEARTBEAT frame.
 */
export function encodeHeartbeat() {
  const frame = Buffer.allocUnsafe(HEADER_SIZE);
  writeHeader(frame, MsgType.HEARTBEAT, 0, 0n);
  return frame;
}

/**
 * Returns true if a topic pattern matches a concrete topic.
 * Supports trailing wildcard: "order.*" matches "order.created".
 */
export function topicMatches(pattern, topic) {
  if (pattern === topic) return true;
  if (pattern === '*')   return true;
  if (pattern.endsWith('.*')) {
    const prefix = pattern.slice(0, -1); // "order."
    return topic.startsWith(prefix);
  }
  return false;
}

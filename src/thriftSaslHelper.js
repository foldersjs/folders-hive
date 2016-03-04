/*
 * A thrift Sasl helper functions.
 * used for conn handshake using SASL.
 * The Thrift SASL communication may reference the doc.
 * https://github.com/apache/thrift/blob/master/doc/specs/thrift-sasl-spec.txt
 * 
 * A Thrift SASL handshake message shall be a byte array of the following form:
 * | 1-byte status code | 4-byte payload length | variable-length payload |
 * The length fields shall be interpreted as integers, with the high byte sent first (Big Endian)
 * 
 * The following message after handshake success
 * | 4-byte payload length | variable-length payload | 
 * prefixed by the 4-byte length of the payload data, followed by the payload
 */

var STATUS_BYTES = 1;
var PAYLOAD_LENGTH_BYTES = 4;

// NegotiationStatus node in handshake
var NegotiationStatus = {
  START : 0x01,
  OK : 0x02,
  BAD : 0x03,
  ERROR : 0x04,
  COMPLETE : 0x05
}

var bufferify = function(val) {
  return Buffer.isBuffer(val) ? val : new Buffer(val);
};

// wrap sasl handshake message, | 1-byte status code | 4-byte payload length | variable-length payload |
var wrapSaslMessage = function(status, payload) {
  if (!payload)
    payload = '';
  payload = bufferify(payload);

  var message = new Buffer(STATUS_BYTES + PAYLOAD_LENGTH_BYTES + payload.length);
  message[0] = status;
  message.writeInt32BE(payload.length, STATUS_BYTES);
  payload.copy(message, STATUS_BYTES + PAYLOAD_LENGTH_BYTES);
  return message;
}

// buffer to store the recv data
var responseBuffer = new Buffer([]);

var appendToBuffer = function(dataBuf) {
  var old = responseBuffer;
  responseBuffer = new Buffer(old.length + dataBuf.length);
  old.copy(responseBuffer, 0);
  dataBuf.copy(responseBuffer, old.length);
  return responseBuffer;
}

// parse a whole sasl handshake message,
// retrun false if the message recved un-completed
// | 1-byte status code | 4-byte payload length | variable-length payload |
var parseSaslMessage = function(dataBuf) {
  dataBuf = bufferify(dataBuf);

  if (dataBuf.length < STATUS_BYTES + PAYLOAD_LENGTH_BYTES) {
    return false;
  }

  var status = dataBuf.readUInt8(0);
  var payloadLength = dataBuf.readUInt32BE(STATUS_BYTES);

  // ensure to recv all the payload
  if (dataBuf.length < STATUS_BYTES + PAYLOAD_LENGTH_BYTES + payloadLength) {
    return false;
  }

  // a whole handshake response recv
  var payload = dataBuf.slice(STATUS_BYTES + PAYLOAD_LENGTH_BYTES);

  if (status == NegotiationStatus.BAD || status == NegotiationStatus.ERROR) {
    console.error("Peer indicated failure: ", payload.toString());
  }

  // Reset the responseBuffer
  responseBuffer = responseBuffer.slice(STATUS_BYTES + PAYLOAD_LENGTH_BYTES + payloadLength);

  return {
    status : status,
    payload : payload
  };
}

// handshake on the conn
var saslPlainHandleShake = function(connection, options, cb) {

  // remove the exist data listener in thrift connection.
  var dataListeners = connection.listeners('data');
  connection.removeAllListeners('data');

  var callback = function(error) {
    // restore the data listener in thrift connection when handshake finished
    for (var i = 0; i < dataListeners.length; i++) {
      connection.addListener('data', dataListeners[i]);
    }
    cb(error);
  }

  var authRspListener = function(dataBuf) {
    var response = parseSaslMessage(appendToBuffer(dataBuf));
    while (response) {
      if (response.status == NegotiationStatus.OK) {
        // FIXME not expected OK in PLAIN sasl
      } else if (response.status == NegotiationStatus.COMPLETE) {
        console.log('[ThriftSaslHelper] COMPLETE message received, PLAIN SASL handshaked success');

        // remove this handshake data handler.
        connection.removeListener('data', authRspListener);
        callback(null);
      } else {
        console.error("error status code, ", response.status, response.payload.toString());

        // remove this handshake data handler.
        connection.removeListener('data', authRspListener);
        callback(response.payload.toString());
      }
      // continue fetch the next response message from buffer
      response = parseSaslMessage(responseBuffer);
    }
  }

  // listen to the handshake response,
  connection.on('data', authRspListener);

  console.log('[ThriftSaslHelper] send START Message,');
  connection.write(wrapSaslMessage(NegotiationStatus.START, 'PLAIN'));

  var authStr = '\0' + options.username + '\0' + options.password;
  console.log('[ThriftSaslHelper] send PLAIN SASL auth Message,');
  connection.write(wrapSaslMessage(NegotiationStatus.COMPLETE, authStr));

}

exports.saslPlainHandleShake = saslPlainHandleShake;
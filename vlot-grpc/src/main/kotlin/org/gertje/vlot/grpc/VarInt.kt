package org.gertje.vlot.grpc

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

// This file contains functions to encode and decode variable length integers.

/**
 * Writes a long as a variable length integer. The integer is first zigzag encoded, to make sure that small negative
 * integers don't use massive amounts of bytes.
 */
fun DataOutput.writeVarInt(value: Long) {
    this.writeRawVarInt(value.encodeZigZag())
}

/**
 * Encodes a signed int as an unsigned int in a zig-zag fashion;
 * `0 = 0, -1 = 1, 1 = 2, -2 = 3, 2 = 4, -3 = 5, 3 = 6 ...`
 */
@Suppress("MagicNumber")
private fun Long.encodeZigZag(): Long {
    return this shl 1 xor (this shr 63)
}


const val MASK = 0x7FL // long with only zeroes at the lowest 7 bits
const val INV_MASK = 0x7FL.inv() // long with only zeroes at the lowest 7 bits
const val CONTINUATION_BIT = 0x80L

/**
 * Writes a long as a variable length integer, where the integer does not get zigzag encoded first.
 */
@Suppress("MagicNumber")
fun DataOutput.writeRawVarInt(value: Long) {
    var l = value
    while (true) {
        // Check if this is the last part to be written
        l = if (l and INV_MASK == 0L) {
            this.write(l.toInt())
            return
        } else {
            this.write((l and MASK or CONTINUATION_BIT).toInt())
            l ushr 7
        }
    }
}

/**
 * Reads a variable length encoded integer and zigzag decodes the result.
 */
fun DataInput.readVarInt(): Long {
    return this.readRawVarInt().decodeZigZag()
}

/**
 * Decodes an unsigned int as a signed int in a zig-zag fashion;
 * `0 = 0, 1 = -1, 2 = 1, 3 = -2, 4 = 2, 5 = -3, 6 = 3 ...`
 */
private fun Long.decodeZigZag(): Long {
    return this ushr 1 xor -(this and 1)
}

/**
 * Reads a variable length encoded integer, without any further decoding.
 */
@Suppress("MagicNumber")
fun DataInput.readRawVarInt(): Long {
    var shift = 0
    var result: Long = 0
    while (shift < 64) {
        val b = this.readByte().toLong()
        result = result or (b and MASK shl shift)
        if (b and CONTINUATION_BIT == 0L) {
            return result
        }
        shift += 7
    }
    throw IOException("Malformed VarInt")
}

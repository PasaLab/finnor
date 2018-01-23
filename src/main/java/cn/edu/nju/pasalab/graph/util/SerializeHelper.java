package cn.edu.nju.pasalab.graph.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by wangzhaokang on 6/18/17.
 */
public class SerializeHelper {

   public static byte[] longToBytes(long vid) {
        byte result[] = {
                (byte) (vid & 0xFF),
                (byte) (vid >> 8 & 0xFF),
                (byte) (vid >> 16 & 0xFF),
                (byte) (vid >> 24 & 0xFF),
                (byte) (vid >> 32 & 0xFF),
                (byte) (vid >> 40 & 0xFF),
                (byte) (vid >> 48 & 0xFF),
                (byte) (vid >> 56 & 0xFF),
                        };
        return result;
    }

    public static byte[] longsToBytes(long array[]) {
        ByteBuffer buf = ByteBuffer.allocate((array.length + 1) * 8);

        // The first int is the array length
        buf.putInt(array.length);

        long prev = 0;
        for (long next : array) {
            if (next - prev <= Integer.MAX_VALUE) {
                // Delta is small. Change the sign and encode as int.
                buf.putInt((int) (prev - next));
            } else {
                // Delta does not fit in 31 bits. Encode two parts of long.
                buf.putInt((int) (next >>> 32));
                buf.putInt((int) next);
            }
            prev = next;
        }
        return Arrays.copyOfRange(buf.array(), 0, buf.position());
    }


    public static long[] bytesToLongs(byte bytes[]) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        // The first int is the array length
        long[] array = new long[buf.getInt()];

        long next = 0;
        for (int i = 0; i < array.length; i++) {
            int delta = buf.getInt();
            if (delta <= 0) {
                // Negative sign means the value is encoded as int delta.
                next -= delta;
            } else {
                // Positive sign means the value is encoded as raw long.
                // Read the second (lower) part of long and combine it with the higher part.
                next = (long) delta << 32 | (buf.getInt() & 0xffffffffL);
            }
            array[i] = next;
        }
        return array;
    }

    public static long bytesToLong(byte bytes[]) {
       long result = 0;
       result = (long)bytes[0]
               | ((long)bytes[1] << 8)
               | ((long)bytes[2] << 16)
               | ((long)bytes[3] << 24)
               | ((long)bytes[4] << 32)
               | ((long)bytes[5] << 40)
               | ((long)bytes[6] << 48)
               | ((long)bytes[7] << 56);
       return result;
   }

}

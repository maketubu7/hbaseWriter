package com.bbd.utils;

import com.google.common.base.Preconditions;

public class LongEncoding {
    private static final String BASE_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyz";

    public LongEncoding() {
    }

    public static long decode(String s) {
        return decode(s, "0123456789abcdefghijklmnopqrstuvwxyz");
    }

    public static String encode(long num) {
        return encode(num, "0123456789abcdefghijklmnopqrstuvwxyz");
    }

    public static long decode(String s, String symbols) {
        int B = symbols.length();
        long num = 0L;
        char[] var5 = s.toCharArray();
        int var6 = var5.length;

        for(int var7 = 0; var7 < var6; ++var7) {
            char ch = var5[var7];
            num *= (long)B;
            int pos = symbols.indexOf(ch);
            if (pos < 0) {
                throw new NumberFormatException("Symbol set does not match string");
            }

            num += (long)pos;
        }

        return num;
    }

    public static String encode(long num, String symbols) {
        Preconditions.checkArgument(num >= 0L, "Expected non-negative number: " + num);
        int B = symbols.length();

        StringBuilder sb;
        for(sb = new StringBuilder(); num != 0L; num /= (long)B) {
            sb.append(symbols.charAt((int)(num % (long)B)));
        }

        return sb.reverse().toString();
    }

    public static void main(String[] args) {
        System.out.println(decode("person"));
    }

}

package com.colbert.hadoop.utils;

import org.apache.commons.lang.StringUtils;

/**
 * @Description 过滤非中文字符的工具类
 * @Date 2021/1/1 18:46
 * @Author Colbert
 */
public class Filter {
    /**
     * 判断字符串是否为中文
     * @param string 要判断的字符串
     * @return 如果是中文就返回true，否则返回flas
     */
    public static boolean chineseFilter(String string) {
        if (StringUtils.isEmpty(string)) {
            return false;
        }
        int n;
        // 遍历字符串，判断每一个字符是否为中文字符
        for (int i = 0; i < string.length(); i++) {
            n = string.charAt(i);
            if (!(19968 <= n && n < 40869)) {
                return false;
            }
        }
        return true;
    }
}

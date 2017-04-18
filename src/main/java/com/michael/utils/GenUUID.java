package com.michael.utils;

import java.util.UUID;

/**
 * Created by hadoop on 17-4-14.
 */
public class GenUUID {
    public static String getUUID(){
        UUID u = UUID.randomUUID();
        return u.toString();
    }
}

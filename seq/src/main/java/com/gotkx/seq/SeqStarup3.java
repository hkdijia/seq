package com.gotkx.seq;

import bean.SeqConfig;
import thirdpart.codec.BodyCodec;

import java.io.IOException;

/**
 * @author HuangKai
 */
public class SeqStarup3 {

    public static void main(String[] args) throws IOException {
        String configName = "seq3.properties";
        new SeqConfig(configName,new BodyCodec()).start();
    }

}

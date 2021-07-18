package com.gotkx.seq;

import bean.SeqConfig;
import thirdpart.codec.BodyCodec;

import java.io.IOException;

/**
 * @author HuangKai
 */
public class SeqStarup1 {

    public static void main(String[] args) throws IOException {
        String configName = "seq1.properties";
        new SeqConfig(configName,new BodyCodec()).start();
    }

}

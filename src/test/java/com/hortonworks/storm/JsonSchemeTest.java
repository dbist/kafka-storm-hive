/*
 * Copyright 2015 aervits.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.storm;

import backtype.storm.tuple.Fields;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author aervits
 */
public class JsonSchemeTest {

    private static final String json = "{\"id\":\"97c48513-4d35-4073-9640-a6e639507bff\",\"person\":{\"first_name\":\"Carl\",\"last_name\":\"Meyer\",\"email\":\"cmeyerf@scribd.com\",\"location\":{\"address\":\"3500 Continental Court\",\"city\":\"Herndon\",\"state\":\"Virginia\",\"zipcode\":\"22070\"},\"text\":\"eros suspendisse accumsan tortor quis turpis sed\",\"url\":\"https://usda.gov/leo/maecenas/pulvinar/lobortis/est/phasellus/sit.js?ipsum=vel\\u0026integer=augue\\u0026a=vestibulum\\u0026nibh=rutrum\\u0026in=rutrum\\u0026quis=neque\\u0026justo=aenean\\u0026maecenas=auctor\\u0026rhoncus=gravida\\u0026aliquam=sem\\u0026lacus=praesent\\u0026morbi=id\\u0026quis=massa\\u0026tortor=id\\u0026id=nisl\\u0026nulla=venenatis\\u0026ultrices=lacinia\\u0026aliquet=aenean\\u0026maecenas=sit\\u0026leo=amet\\u0026odio=justo\\u0026condimentum=morbi\\u0026id=ut\\u0026luctus=odio\\u0026nec=cras\\u0026molestie=mi\\u0026sed=pede\\u0026justo=malesuada\\u0026pellentesque=in\\u0026viverra=imperdiet\\u0026pede=et\\u0026ac=commodo\\u0026diam=vulputate\\u0026cras=justo\\u0026pellentesque=in\\u0026volutpat=blandit\\u0026dui=ultrices\\u0026maecenas=enim\\u0026tristique=lorem\\u0026est=ipsum\\u0026et=dolor\\u0026tempus=sit\\u0026semper=amet\\u0026est=consectetuer\\u0026quam=adipiscing\\u0026pharetra=elit\\u0026magna=proin\\u0026ac=interdum\\u0026consequat=mauris\\u0026metus=non\\u0026sapien=ligula\\u0026ut=pellentesque\\u0026nunc=ultrices\\u0026vestibulum=phasellus\\u0026ante=id\\u0026ipsum=sapien\\u0026primis=in\\u0026in=sapien\\u0026faucibus=iaculis\\u0026orci=congue\\u0026luctus=vivamus\\u0026et=metus\\u0026ultrices=arcu\\u0026posuere=adipiscing\\u0026cubilia=molestie\\u0026curae=hendrerit\\u0026mauris=at\\u0026viverra=vulputate\\u0026diam=vitae\\u0026vitae=nisl\\u0026quam=aenean\\u0026suspendisse=lectus\\u0026potenti=pellentesque\\u0026nullam=eget\\u0026porttitor=nunc\\u0026lacus=donec\\u0026at=quis\\u0026turpis=orci\\u0026donec=eget\\u0026posuere=orci\\u0026metus=vehicula\\u0026vitae=condimentum\\u0026ipsum=curabitur\\u0026aliquam=in\\u0026non=libero\\u0026mauris=ut\\u0026morbi=massa\\u0026non=volutpat\\u0026lectus=convallis\\u0026aliquam=morbi\\u0026sit=odio\\u0026amet=odio\"}}";


    public JsonSchemeTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of deserialize method, of class JsonScheme.
     */
    @Test
    public void testDeserializeString() {
        System.out.println("deserializeFromString");
        byte[] bytes = json.getBytes();
        JsonScheme instance = new JsonScheme();
        List<Object> expResult = new ArrayList<>();
        expResult.add("97c48513-4d35-4073-9640-a6e639507bff");
        expResult.add("Carl");
        expResult.add("Meyer");
        expResult.add("cmeyerf@scribd.com");
        expResult.add("3500 Continental Court");
        expResult.add("Herndon");
        expResult.add("Virginia");
        expResult.add("22070");
        expResult.add("eros suspendisse accumsan tortor quis turpis sed");
        expResult.add("https://usda.gov/leo/maecenas/pulvinar/lobortis/est/phasellus/sit.js?ipsum=vel\\u0026integer=augue\\u0026a=vestibulum\\u0026nibh=rutrum\\u0026in=rutrum\\u0026quis=neque\\u0026justo=aenean\\u0026maecenas=auctor\\u0026rhoncus=gravida\\u0026aliquam=sem\\u0026lacus=praesent\\u0026morbi=id\\u0026quis=massa\\u0026tortor=id\\u0026id=nisl\\u0026nulla=venenatis\\u0026ultrices=lacinia\\u0026aliquet=aenean\\u0026maecenas=sit\\u0026leo=amet\\u0026odio=justo\\u0026condimentum=morbi\\u0026id=ut\\u0026luctus=odio\\u0026nec=cras\\u0026molestie=mi\\u0026sed=pede\\u0026justo=malesuada\\u0026pellentesque=in\\u0026viverra=imperdiet\\u0026pede=et\\u0026ac=commodo\\u0026diam=vulputate\\u0026cras=justo\\u0026pellentesque=in\\u0026volutpat=blandit\\u0026dui=ultrices\\u0026maecenas=enim\\u0026tristique=lorem\\u0026est=ipsum\\u0026et=dolor\\u0026tempus=sit\\u0026semper=amet\\u0026est=consectetuer\\u0026quam=adipiscing\\u0026pharetra=elit\\u0026magna=proin\\u0026ac=interdum\\u0026consequat=mauris\\u0026metus=non\\u0026sapien=ligula\\u0026ut=pellentesque\\u0026nunc=ultrices\\u0026vestibulum=phasellus\\u0026ante=id\\u0026ipsum=sapien\\u0026primis=in\\u0026in=sapien\\u0026faucibus=iaculis\\u0026orci=congue\\u0026luctus=vivamus\\u0026et=metus\\u0026ultrices=arcu\\u0026posuere=adipiscing\\u0026cubilia=molestie\\u0026curae=hendrerit\\u0026mauris=at\\u0026viverra=vulputate\\u0026diam=vitae\\u0026vitae=nisl\\u0026quam=aenean\\u0026suspendisse=lectus\\u0026potenti=pellentesque\\u0026nullam=eget\\u0026porttitor=nunc\\u0026lacus=donec\\u0026at=quis\\u0026turpis=orci\\u0026donec=eget\\u0026posuere=orci\\u0026metus=vehicula\\u0026vitae=condimentum\\u0026ipsum=curabitur\\u0026aliquam=in\\u0026non=libero\\u0026mauris=ut\\u0026morbi=massa\\u0026non=volutpat\\u0026lectus=convallis\\u0026aliquam=morbi\\u0026sit=odio\\u0026amet=odio");
        List<Object> result = instance.deserialize(bytes);
        assertEquals(expResult.get(0), result.get(0));
        assertEquals(expResult.get(1), result.get(1));
        assertEquals(expResult.get(2), result.get(2));
        assertEquals(expResult.get(3), result.get(3));
        assertEquals(expResult.get(4), result.get(4));
        assertEquals(expResult.get(5), result.get(5));
        assertEquals(expResult.get(6), result.get(6));
        assertEquals(expResult.get(7), result.get(7));
        assertEquals(expResult.get(8), result.get(8));
        assertEquals(expResult, result);
    }

    /**
     * Test of deserialize method, of class JsonScheme.
     */
    @Test
    public void testDeserializeFromFile() {
        System.out.println("deserializeFromFile");
        byte[] bytes = null;
        Charset charset = Charset.forName("UTF-8");
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("src/resources/sample2.json"), charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                bytes = line.getBytes();
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }

        JsonScheme instance = new JsonScheme();
        List<Object> expResult = new ArrayList<>();
        expResult.add("408ee2ae-2979-4829-8004-c7947911fdfe");
        expResult.add("Russell");
        expResult.add("Lynch");
        expResult.add("rlynchg@ft.com");
        expResult.add("55032 Ryan Drive");
        expResult.add("Miami");
        expResult.add("Florida");
        expResult.add("33164");
        expResult.add("eget elit sodales scelerisque mauris sit");
        expResult.add("http://angelfire.com/mauris/lacinia/sapien/quis.jsp?velit=ut\\u0026donec=nulla\\u0026diam=sed\\u0026neque=accumsan\\u0026vestibulum=felis\\u0026eget=ut\\u0026vulputate=at\\u0026ut=dolor\\u0026ultrices=quis\\u0026vel=odio\\u0026augue=consequat\\u0026vestibulum=varius\\u0026ante=integer\\u0026ipsum=ac\\u0026primis=leo\\u0026in=pellentesque\\u0026faucibus=ultrices\\u0026orci=mattis\\u0026luctus=odio\\u0026et=donec\\u0026ultrices=vitae\\u0026posuere=nisi\\u0026cubilia=nam\\u0026curae=ultrices\\u0026donec=libero\\u0026pharetra=non\\u0026magna=mattis\\u0026vestibulum=pulvinar\\u0026aliquet=nulla\\u0026ultrices=pede\\u0026erat=ullamcorper\\u0026tortor=augue\\u0026sollicitudin=a\\u0026mi=suscipit\\u0026sit=nulla\\u0026amet=elit\\u0026lobortis=ac\\u0026sapien=nulla\\u0026sapien=sed\\u0026non=vel\\u0026mi=enim\\u0026integer=sit\\u0026ac=amet\\u0026neque=nunc");

        List<Object> result = instance.deserialize(bytes);
        assertEquals(expResult.get(0), result.get(0));
        assertEquals(expResult, result);

    }

    /**
     * Test of getOutputFields method, of class JsonScheme.
     */
    @Test
    public void testGetOutputFields() {
        System.out.println("getOutputFields");
        JsonScheme instance = new JsonScheme();
        Fields expResult = new Fields("id", "firstName", "lastName", "email",
                "address", "city", "state", "zipcode", "text", "url");

        Fields result = instance.getOutputFields();

        assertEquals(expResult.get(0), result.get(0));
        assertEquals(expResult.get(1), result.get(1));
        assertEquals(expResult.get(2), result.get(2));
        assertEquals(expResult.get(3), result.get(3));
        assertEquals(expResult.get(4), result.get(4));
        assertEquals(expResult.get(5), result.get(5));
        assertEquals(expResult.get(6), result.get(6));
        assertEquals(expResult.get(7), result.get(7));
        assertEquals(expResult.get(8), result.get(8));
        assertEquals(expResult.get(9), result.get(9));
    }

}

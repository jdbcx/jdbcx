package io.github.jdbcx.script;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ScriptHelperTest {
    private final ScriptHelper helper = ScriptHelper.getInstance();

    @Test(groups = { "unit" })
    public void testLoad() throws IOException {
        Assert.assertEquals(helper.load(null), "");
        Assert.assertTrue(helper.load("").length() > 0); // list files in current directory
        Assert.assertTrue(helper.load("target/test-classes/test-config.properties").length() > 0);
    }
}

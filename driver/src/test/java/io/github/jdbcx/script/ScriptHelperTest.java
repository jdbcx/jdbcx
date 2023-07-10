package io.github.jdbcx.script;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ScriptHelperTest {
    private final ScriptHelper helper = ScriptHelper.getInstance();

    @Test(groups = { "unit" })
    public void testLoad() throws IOException {
        Assert.assertEquals(helper.read(null), "");
        Assert.assertThrows(FileNotFoundException.class, () -> helper.read("non-existing-file"));
        Assert.assertTrue(helper.read("").length() > 0); // list files in current directory
        Assert.assertTrue(helper.read("target/test-classes/test-config.properties").length() > 0);
        Assert.assertTrue(helper.read("file:target/test-classes/test-config.properties").length() > 0);
    }
}

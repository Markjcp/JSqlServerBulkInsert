// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class VarcharMappingTest extends TransactionalTestBase {

    private class VarcharEntity {

        private final String value;

        public VarcharEntity(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private class VarcharEntityMapping extends AbstractMapping<VarcharEntity> {

        public VarcharEntityMapping() {
            super("dbo", "UnitTest");

            mapVarchar("StringValue", 30,VarcharEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        String stringData = "Halli Hallo Hallöchen";
        // Create te
        List<VarcharEntity> entities = Arrays.asList(new VarcharEntity(stringData));
        // Create the BulkInserter:
        VarcharEntityMapping mapping = new VarcharEntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        String resultString = rs.getString("StringValue");
        // Assert both are equal:
        Assert.assertEquals(stringData, resultString);
        // Assert only one record was read:
        Assert.assertEquals(false, rs.next());
    }

    private ResultSet getAll() throws SQLException {

        String sqlStatement = "SELECT * FROM dbo.UnitTest";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private void createTestTable() throws SQLException {
        String sqlStatement = "CREATE TABLE [dbo].[UnitTest]\n" +
                "            (\n" +
                "                StringValue VARCHAR(30)\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}

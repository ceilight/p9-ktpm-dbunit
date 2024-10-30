package DBUnitDemoTest;

import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DBUnitDemoTest {
  private IDatabaseTester databaseTester;
  private Connection connection;

  @BeforeEach
  public void setUp() throws Exception {
    databaseTester = new JdbcDatabaseTester(
            "org.postgresql.Driver",
            "jdbc:postgresql://localhost:5432/dummy1",
            "postgres",
            "1738.imlikeheywassuphello"
    );
    IDataSet dataSet = new FlatXmlDataSetBuilder()
            .build(new FileInputStream("dataset.xml"));
    databaseTester.setDataSet(dataSet);
    databaseTester.setSetUpOperation(DatabaseOperation.REFRESH);
    databaseTester.onSetup();

    connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/dummy1", "postgres", "1738.imlikeheywassuphello");
  }

  @AfterEach
  public void tearDown() throws Exception {
    databaseTester.setTearDownOperation(DatabaseOperation.DELETE_ALL);
    databaseTester.onTearDown();
    connection.close();
  }

  @Test
  public void testStudentCount() throws Exception {
    int expectedCount = 4;

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery("SELECT COUNT(1) FROM student");
    rs.next();

    assertThat(rs.getInt("count"), is(expectedCount));
  }

  @Test
  public void testQueryById() throws SQLException {
    int idTest = 3;
    String expectedName = "Carol";
    double expectedGPA = 3.9;
    int expectedCredits = 40;

    Statement statement = connection.createStatement();
    String query = "SELECT * FROM student WHERE id=" + idTest;
    ResultSet rs = statement.executeQuery(query);
    rs.next();

    assertThat(rs.getString("name"), is(expectedName));
    assertThat(rs.getDouble("gpa"), is(expectedGPA));
    assertThat(rs.getInt("credits"), is(expectedCredits));
  }

  @Test
  public void testQueryByCreditThreshold() throws SQLException {
    int creditThreshold = 50;
    int expectedCount = 2;

    Statement statement = connection.createStatement();
    String query = "SELECT COUNT(1) FROM student WHERE credits>=" + creditThreshold;
    ResultSet rs = statement.executeQuery(query);
    rs.next();

    assertThat(rs.getInt("count"), is(expectedCount));
  }
}

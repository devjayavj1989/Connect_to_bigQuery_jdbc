import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.*;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;
import java.util.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Locale;
import java.util.Scanner;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import org.apache.beam.sdk.extensions.jackson.AsJsons;


public class JacksonTester {

    public static void main(String args[]){



        //ObjectMapper mapper = new ObjectMapper();
        // jsonString = "{\"name\":\"Mahesh\", \"age\":21}";

        //map json to student
        try{
 //           Student student = mapper.readValue(jsonString,Student.class);
//
//                    System.out.println(student);
//
//            jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(student);
//
//            System.out.println("jsonstring"+jsonString);
            TableSchema schema = new TableSchema().setFields(Arrays.asList(
                    new TableFieldSchema().setName("FirstName").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("LastName").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("CategoryName").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("FilmID").setType("INTEGER").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Title").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Description").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("ReleaseYear").setType("INTEGER").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Language_id").setType("INTEGER").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Rental_duration").setType("INTEGER").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Rental_rate").setType("INTEGER").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Length").setType("INTEGER").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Replacement_cost").setType("INTEGER").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Rating").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("last_updated").setType("TIMESTAMP").setMode("NULLABLE"),
                    new TableFieldSchema().setName("Special_features").setType("STRING").setMode("REPEATED"),
                    new TableFieldSchema().setName("Fulltext").setType("STRING").setMode("NULLABLE")));











            Scanner sn = new Scanner(System.in);
            String searchFinder ="'"+sn.nextLine()+"%'";
            String queryString=("select a.first_name as first_name,a.last_name ,c.name as category_name,f.*\n" +
                    "from actor as a join film_actor as fa\n" +
                    "on a.actor_id=fa.actor_id \n" +
                    "join film as f \n" +
                    "on f.film_id=fa.film_id \n" +
                    "join film_category as fc\n" +
                    "on f.film_id=fc.film_id\n" +
                    "join category as c \n" +
                    "on fc.category_id=c.category_id where a.first_name like "+searchFinder+"or a.last_name like "+searchFinder+" limit 250");

            System.out.println(queryString);
//            DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
//            options.setJobName("FilmTableRead");
//            options.setRunner(DataflowRunner.class);
//            options.setProject("york-cdf-start");
//            options.setRegion("us-central1");
            PipelineOptions options =PipelineOptionsFactory.create();
            options.setTempLocation("gs://jaya_final_capstone_test_bucket/temp_file");

            Pipeline p = Pipeline.create(options);
            Class.forName("org.postgresql.Driver");
            // Retrieving data from database
            PCollection<TableRow> data = p.apply(JdbcIO.<TableRow>read()

                    // Data Source Configuration for PostgreSQL
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration

                            // Data Source Configuration for PostgreSQL
                            .create("org.postgresql.Driver","jdbc:postgresql://ls-41d379b19b475ed294babb170cfa0f93b3011e47.cq2f1e9koedo.us-east-2.rds.amazonaws.com/jayavj1989")

                            //Data Source Configuration for MySQL
//						.create("com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/your_db?useSSL=false")

                            .withUsername("dbmasteruser").withPassword("Swnp3XQFtBd)b61NGn!uh{Lw=8#Vk~y<"))

                    .withQuery(queryString)
                    .withCoder(TableRowJsonCoder.of())
                    .withRowMapper(new JdbcIO.RowMapper<TableRow>() {
                        private static final long serialVersionUID = 1L;
                        public TableRow mapRow(ResultSet resultSet) throws Exception {
                            TableRow outputTableRow = new TableRow();
                            outputTableRow.set("FirstName",resultSet.getString("first_name"));
                            outputTableRow.set("LastName",resultSet.getString("last_name"));
                            outputTableRow.set("CategoryName",resultSet.getString("category_name"));
                            outputTableRow.set("FilmID",resultSet.getInt("film_id"));
                            outputTableRow.set("Title",resultSet.getString("title"));
                            outputTableRow.set("Description",resultSet.getString("description"));
                            outputTableRow.set("ReleaseYear",resultSet.getInt("release_year"));
                            outputTableRow.set("Language_id",resultSet.getInt("language_id"));
                            outputTableRow.set("Rental_duration",resultSet.getInt("rental_duration"));
                            outputTableRow.set("Rental_rate",resultSet.getInt("rental_rate"));
                            outputTableRow.set("Length",resultSet.getInt("length"));
                            outputTableRow.set("Replacement_cost",resultSet.getInt("replacement_cost"));
                            outputTableRow.set("Rating",resultSet.getString("rating"));
                            outputTableRow.set("last_updated",resultSet.getString("last_update"));
                           // outputTableRow.set("Special_features",resultSet.getArray("special_features"));
                             outputTableRow.set("Fulltext",resultSet.getString("fulltext"));



                            return outputTableRow;
//                            return "FirstName: "+resultSet.getString("first_name")+
//                                            "\nLastName: "+resultSet.getString("last_name")+"\nCategoryName: "+resultSet.getString("category_name")+"ID: "+resultSet.getInt("film_id")
//                                    +"\nTitle: "+resultSet.getString("title")+
//                                    "\nDescription: "+resultSet.getString("description")+"\nReleaseYear: "+resultSet.getInt("release_year")
//                                    +"\nLanguage_id: "+resultSet.getInt("language_id")+"\nRental_duration: "+resultSet.getInt("rental_duration")
//                                    +"\nRental_rate: "+resultSet.getInt("rental_rate")+"\nReplacement_cost: "+resultSet.getInt("replacement_cost")+"\nRating: "+resultSet.getString("rating")
//                                    +"\nlast_updated: "+resultSet.getTimestamp("last_update")
//                                    +"\nSpecial_features: "+resultSet.getArray("special_features")+"\nFulltext: "+resultSet.getArray("fulltext");


                        }
                    }
            ));
            PCollection<TableRow> words =
     data.apply(ParDo.of(new DoFn<TableRow, TableRow>() {
        @ProcessElement
          public void processElement(ProcessContext c) {
            TableRow line = new TableRow();
            TableRow inputline=c.element();

             if(inputline.containsKey("CategoryName")){
              if((inputline.get("CategoryName").toString()).startsWith("D")){
                  String cn=inputline.get("CategoryName").toString().toUpperCase();
                  //cn.toUpperCase();
                  line.set("CategoryName",cn);
              }
              else if((inputline.get("CategoryName").toString()).startsWith("N")){
                  String cn=inputline.get("CategoryName").toString().toLowerCase();
                  line.set("CategoryName",cn);
              }
              else{
                     line.set("CategoryName",inputline.get("CategoryName"));
           }}
            line.set("FirstName",inputline.get("FirstName"));
           line.set("LastName",inputline.get("LastName"));

            line.set("FilmID",inputline.get("FilmID"));
            if(inputline.containsKey("Title")){

                    String cn=inputline.get("Title").toString().toUpperCase();

                    //cn.toUpperCase();
                    line.set("Title",cn);
                }
           // line.set("Title",inputline.get("Title"));
            line.set("Description",inputline.get("Description"));
            line.set("ReleaseYear",inputline.get("ReleaseYear"));
            line.set("Language_id",inputline.get("Language_id"));
            line.set("Rental_duration",inputline.get("Rental_duration"));
            line.set("Rental_rate",inputline.get("Rental_rate"));
            line.set("Length",inputline.get("Length"));
            line.set("Replacement_cost",inputline.get("Replacement_cost"));
            line.set("Rating",inputline.get("Rating"));
            line.set("last_updated",inputline.get("last_updated"));
            c.output(line);
        }}));
            words.apply(
                    "Write to BigQuery",
                    BigQueryIO.writeTableRows()
                            .to(String.format("%s:%s.%s","york-cdf-start", "jaya_mohan_proj1","film_Search"))
                            .withSchema(schema)
                            // For CreateDisposition:
                            // - CREATE_IF_NEEDED (default): creates the table if it doesn't exist, a schema is
                            // required
                            // - CREATE_NEVER: raises an error if the table doesn't exist, a schema is not needed
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            // For WriteDisposition:
                            // - WRITE_EMPTY (default): raises an error if the table is not empty
                            // - WRITE_APPEND: appends new rows to existing rows
                            // - WRITE_TRUNCATE: deletes the existing rows before writing
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

            // PCollection<TableRow> row_value =data.apply(JsonToRow.withSchema(TransactionFilmTable)).apply(Convert.to(TableRow.class));
//
//            System.out.println(data_json.expand().values());
           // data.apply(TextIO.write().to("gs://jaya_final_capstone_test_bucket/postgresql.txt"));
            p.run().waitUntilFinish();
        }
        catch(Exception e){ System.out.println(e);}
//        catch (JsonParseException e) { e.printStackTrace();}
//        catch (JsonMappingException e) { e.printStackTrace(); }
//        catch (IOException e) { e.printStackTrace(); }
    }
}
//@DefaultSchema(JavaFieldSchema.class)
//public class TransactionFilmTable {
//    public final String FirstName;
//    public final String Lastname;
//    public final String categoryName;
//    public final int ID;
//
//    public final double purchaseAmount;
//    @SchemaCreate
//    public TransactionPojo(String bank, double purchaseAmount) {
//        this.bank = bank;
//        this.purchaseAmount = purchaseAmount;
//    }
//}

class Student {
    private String name;
    private int age;
    public Student(){}
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
    public String toString(){
        return "Student [ name: "+name+", age: "+ age+ " ]";
    }
}
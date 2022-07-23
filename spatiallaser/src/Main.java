import java.sql.*;

public class Main {
    // This class has functions to calculate total population and average income using both areal and point-in-polygon interpolation
    // The point coordinates and buffer size can be specified for each function

    public static void areal(Connection c, double lng, double lat, float buff) throws SQLException { // areal interpolation
        // create buffer, calculate percentage of area in buffer, filter for counties that intersect with buffer
        String sql = """
                select *, (st_area(st_intersection(county, buff)) / st_area(county)) as percentage
                from (
                    select income, population, "SpatialObj" as county, ST_Buffer(ST_MakePoint(?, ?)::geography, ?)::geometry as buff
                    from dfw_demo
                ) as d
                where st_intersects(county, buff)  = true;""";

        PreparedStatement st = c.prepareStatement(sql);
        // set point coordinates and buffer size
        st.setDouble(1, lng);
        st.setDouble(2, lat);
        st.setFloat(3, buff);
        ResultSet r = st.executeQuery();

        int totalPop = 0;
        long totalInc = 0;

        while(r.next()) { // add up population and income
            int income = r.getInt("income");
            int pop = r.getInt("population");
            float pct = r.getFloat("percentage");

            totalPop += pop * pct;
            totalInc += income * (pop * pct); // get population within buffer (assume an even distribution), assume everyone in a county makes the average amount
        }
        // close connections and print results
        r.close();
        st.close();

        System.out.println("Areal interpolation");
        System.out.println("Total population: " + totalPop);
        System.out.println("Average income: " + ((totalPop == 0) ? 0 : totalInc/totalPop)); // calculate average income
    }
    public static void pip(Connection c, double lng, double lat, float buff) throws SQLException { // point in polygon interpolation
        // create buffer, calculate centroid, filter for centroids within buffer
        String sql = """
                select income, population
                from dfw_demo as d
                where st_within(st_centroid(d."SpatialObj"), ST_Buffer(ST_MakePoint(?, ?)::geography, ?)::geometry) = true;""";

        PreparedStatement st = c.prepareStatement(sql);
        // set point coordinates and buffer size
        st.setDouble(1, lng);
        st.setDouble(2, lat);
        st.setFloat(3, buff);
        ResultSet r = st.executeQuery();

        int totalPop = 0;
        long totalInc = 0;

        while(r.next()) { // add up population and income
            int income = r.getInt("income");
            int pop = r.getInt("population");

            totalPop += pop;
            totalInc += income * pop; // assume everyone in a county makes the average amount
        }
        // close connections and print results
        r.close();
        st.close();

        System.out.println("Point-in-polygon interpolation");
        System.out.println("Total population: " + totalPop);
        System.out.println("Average income: " + ((totalPop == 0) ? 0 : totalInc/totalPop)); // calculate average income
    }

    public static void main(String[] args) {
        String url = "jdbc:postgresql://3.235.170.15:5432/gis";
        String user = "guest";
        String password = "U8OPtddp";

        double latitude = 33.045352;
        double longitude = -96.781508;
        float buffer = 2000;

        System.out.println(buffer/1000 + " km buffer around (" + latitude + ", " + longitude + ")\n");

        try {
            Connection c = DriverManager.getConnection(url, user, password); // connect to database
            pip(c, longitude, latitude, buffer); // point-in-polygon interpolation
            System.out.println();
            areal(c, longitude, latitude, buffer); // areal interpolation
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
    }
}
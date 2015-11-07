package middleware.core;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import middleware.db.MySqlDBConnectionFactory;

public class MiddlewareJP3 {
	
	/**
	 * El valor que representa un error en la medicion de temperatura, por parte de las estaciones de monitoreo.
	 */
	private static final BigDecimal VALUE_ERROR_MEASURING = BigDecimal.valueOf(1802.6);
	
	
	private static final String START_PERIOD = "2011-01-01";
	

	private static final String END_PERIOD = "2011-12-31";
	
	private static final String END_AUTUMN = "2011-06-21";
	private static final String END_WINTER = "2011-09-21";
	private static final String END_SPRING = "2011-12-21";
	private static final String END_SUMMER = "2011-03-21";
	
	
	
	/**
	 * Formatters para las fechas y valores decimales
	 */
	private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
	private static final DecimalFormat DECIMAL_FORMATTER = new DecimalFormat("#.0"); 

	
	
    /**
     * Temperatura maxima, minima, media y cantidad de lluvia caida para lo 'N' dias en los que hubo menos viajes en bicicletas
     * 
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // Obtenemos las conexiones de las Bases de Datos: Meteorologia y Metro
        Connection meteorologiaDBConn = MySqlDBConnectionFactory.METEOROLOGIA_DB.getConnection();
        Connection metroDBConn = MySqlDBConnectionFactory.METRO_DB.getConnection();
       
        PreparedStatement preparedStatement = null;
        ResultSet avgTemp = null ;
        ResultSet rushHourRs = null;
		try {
			preparedStatement = buildQueryRushHour(metroDBConn);
	        rushHourRs = preparedStatement.executeQuery();
	        
	        Time hour = null;
//	        Integer totalTravels;
	        while (rushHourRs.next()) {
	        	hour = rushHourRs.getTime(1);
//				totalTravels = rushHourRs.getInt(2);
			}
	        
	        System.out.println("La hora del dia en el que viajan mas personas en metro es: " + hour.toString());
	        
			preparedStatement = buildQueryTempAvgAutumn(meteorologiaDBConn, hour);
			avgTemp = preparedStatement.executeQuery();
			printResults(avgTemp, "Otoño", hour); 
			
			preparedStatement = buildQueryTempAvgWinter(meteorologiaDBConn, hour);
			avgTemp = preparedStatement.executeQuery();
			printResults(avgTemp, "Invierno", hour); 
			
			preparedStatement = buildQueryTempAvgSpring(meteorologiaDBConn, hour);
			avgTemp = preparedStatement.executeQuery();
			printResults(avgTemp, "Primavera", hour); 
			
			preparedStatement = buildQueryTempAvgSummer(meteorologiaDBConn, hour);
			avgTemp = preparedStatement.executeQuery();
			printResults(avgTemp, "Verano", hour); 
	        
		} catch (SQLException e) {
			System.err.println("Un error ha ocurrido al intentar acceder a los datos");
			e.printStackTrace();
		} catch (ParseException e) {
			System.err.println("Un error ha ocurrido al intentar dar formato a la fecha " + DATE_FORMATTER);
			e.printStackTrace();
		} finally {
			closeResourcesDB(meteorologiaDBConn, metroDBConn, preparedStatement, avgTemp,
					rushHourRs);
		}
    }


	private static PreparedStatement buildQueryTempAvgWinter(Connection meteorologiaDBConn, Time hour) throws SQLException, ParseException {
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-06-21' and '2011-09-21';
//
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-09-21' and '2011-12-21';
//
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-12-21' and '2011-12-31'
//		or fecha between '2011-01-01' and '2011-03-21';
		
		String h = String.valueOf(hour.getHours()) + ":%";
		PreparedStatement preparedStatement = meteorologiaDBConn.prepareStatement(
				  "select avg(temp_c) from info_metereologica"
				+ " WHERE hora like ? and temp_c <> ?" 
				+ " and fecha between ? and ?");		  
		preparedStatement.setString(1, h);
		preparedStatement.setBigDecimal(2, VALUE_ERROR_MEASURING);
		preparedStatement.setDate(3, new java.sql.Date(DATE_FORMATTER.parse(END_AUTUMN).getTime()));
		preparedStatement.setDate(4, new java.sql.Date(DATE_FORMATTER.parse(END_WINTER).getTime()));
		
		return preparedStatement;
	}
	
	private static PreparedStatement buildQueryTempAvgSpring(Connection meteorologiaDBConn, Time hour) throws SQLException, ParseException {
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-09-21' and '2011-12-21';
//
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-12-21' and '2011-12-31'
//		or fecha between '2011-01-01' and '2011-03-21';
		
		
		String h = String.valueOf(hour.getHours()) + ":%";
		PreparedStatement preparedStatement = meteorologiaDBConn.prepareStatement(
				  "select avg(temp_c) from info_metereologica"
				+ " WHERE hora like ? and temp_c <> ?" 
				+ " and fecha between ? and ?");		  
		preparedStatement.setString(1, h);
		preparedStatement.setBigDecimal(2, VALUE_ERROR_MEASURING);
		preparedStatement.setDate(3, new java.sql.Date(DATE_FORMATTER.parse(END_WINTER).getTime()));
		preparedStatement.setDate(4, new java.sql.Date(DATE_FORMATTER.parse(END_SPRING).getTime()));
		
		return preparedStatement;
	}
	
	private static PreparedStatement buildQueryTempAvgSummer(Connection meteorologiaDBConn, Time hour) throws SQLException, ParseException {
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-12-21' and '2011-12-31'
//		or fecha between '2011-01-01' and '2011-03-21';
		
		String h = String.valueOf(hour.getHours()) + ":%";
		PreparedStatement preparedStatement = meteorologiaDBConn.prepareStatement(
				  "select avg(temp_c) from info_metereologica"
				+ " WHERE hora like ? and temp_c <> ?" 
				+ " and fecha between ? and ?"
				+ " or fecha between ? and ?");		  
		preparedStatement.setString(1, h);
		preparedStatement.setBigDecimal(2, VALUE_ERROR_MEASURING);
		preparedStatement.setDate(3, new java.sql.Date(DATE_FORMATTER.parse(END_SPRING).getTime()));
		preparedStatement.setDate(4, new java.sql.Date(DATE_FORMATTER.parse(END_PERIOD).getTime()));
		preparedStatement.setDate(5, new java.sql.Date(DATE_FORMATTER.parse(START_PERIOD).getTime()));
		preparedStatement.setDate(6, new java.sql.Date(DATE_FORMATTER.parse(END_SUMMER).getTime()));
		
		return preparedStatement;
	}


	/**
	 * @param connection
	 * @param diasConMenosViajes
	 * @return
	 * @throws SQLException
	 * @throws ParseException 
	 */
	private static PreparedStatement buildQueryTempAvgAutumn(Connection connection,
			Time hour) throws SQLException, ParseException {
		
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-03-21' and '2011-06-21';
//
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-06-21' and '2011-09-21';
//
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-09-21' and '2011-12-21';
//
//		select avg(temp_c) from info_metereologica
//		WHERE hora like '18:%' and temp_c <> 1802.6
//		and fecha between '2011-12-21' and '2011-12-31'
//		or fecha between '2011-01-01' and '2011-03-21';
		
		String h = String.valueOf(hour.getHours()) + ":%";
		PreparedStatement preparedStatement = connection.prepareStatement(
				  "select avg(temp_c) from info_metereologica"
				+ " WHERE hora like ? and temp_c <> ?"
				+ " and fecha between ? and ?");		  
		preparedStatement.setString(1, h);
		preparedStatement.setBigDecimal(2, VALUE_ERROR_MEASURING);
		preparedStatement.setDate(3, new java.sql.Date(DATE_FORMATTER.parse(END_SUMMER).getTime()));
		preparedStatement.setDate(4, new java.sql.Date(DATE_FORMATTER.parse(END_AUTUMN).getTime()));
		
		return preparedStatement;
	}

	/**
	 * @param topN
	 * @param connection
	 * @return
	 * @throws SQLException
	 * @throws ParseException 
	 */
	private static PreparedStatement buildQueryRushHour(Connection connection)
			throws SQLException, ParseException {
		
//		SELECT HORA, MAX(TOTAL) as T
//		FROM 
//		(SELECT DATES, HORA, SUM(PAX_TOTAL) AS TOTAL FROM FLUJO_MOLINETES
//		WHERE DATES BETWEEN '2011-03-21' AND '2011-06-21'
//		GROUP BY DATES, HORA) a GROUP by HORA
//		ORDER BY T desc
//		limit 1;
		
		PreparedStatement preparedStatement = connection.prepareStatement(
				  " SELECT HORA, MAX(TOTAL) as T FROM " 
				+ "		( SELECT DATES, HORA, SUM(PAX_TOTAL) AS TOTAL FROM FLUJO_MOLINETES "
				+ " 	  WHERE DATES BETWEEN ? AND ?"  
				+ " 	  GROUP BY DATES, HORA ) a GROUP by HORA"
				+ " ORDER BY T desc "
		        + " limit ?" + ";");
		
		preparedStatement.setDate(1, new java.sql.Date(DATE_FORMATTER.parse(START_PERIOD).getTime()));
		preparedStatement.setDate(2, new java.sql.Date(DATE_FORMATTER.parse(END_PERIOD).getTime()));
		preparedStatement.setInt(3, Integer.valueOf(1));
		
		return preparedStatement;
	}

	/**
	 * Cierra los recursos de DataBase
	 * 
	 * @param meteorologiaDBConn
	 * @param bicicletasDBConn
	 * @param preparedStatement
	 * @param dataMeteorologicaRs
	 * @param topNDiasMenosViajesRs
	 */
	private static void closeResourcesDB(Connection meteorologiaDBConn, Connection bicicletasDBConn,
			PreparedStatement preparedStatement, ResultSet dataMeteorologicaRs, ResultSet topNDiasMenosViajesRs) {
		try {
			dataMeteorologicaRs.close();
			topNDiasMenosViajesRs.close();
			preparedStatement.close();
			meteorologiaDBConn.close();
			bicicletasDBConn.close();
		} catch (Exception e) {
			System.err.println("Un error ha ocurrido al intentar cerrar los recursos de DB");
		}
	}

	/**
	 * Imprime la Temperatura maxima, minima, media y la cantidad de lluvia caida para lo 'N' dias en los que menos viajes en bicicleta hubo 
	 * 
	 * @param dataMeteorologicaRs
	 * @param diasConMenosViajes 
	 * @throws SQLException
	 */
	private static void printResults(ResultSet avgTemp, String season, Time hour) throws SQLException {
		while (avgTemp.next()) {
			double average = avgTemp.getDouble(1);
			System.out.println("La temperatura promedio a las " + hour.toString() + " en " + season + " es de " + DECIMAL_FORMATTER.format(average) + " °C");	
		}
	}

	/**
	 * Obtiene un mapa con aquellos dias en los que hubo menos viajes en bicicleta en la ciudad
	 * 
	 * @param topNDiasMenosViajesRs
	 * @return Un mapa, donde la key es la fecha, y el valor la cantidad de viajes registrados ese dia.
	 * @throws SQLException
	 */
	private static Map<java.sql.Date, Integer> getDiasConMenosViajes(ResultSet topNDiasMenosViajesRs)
			throws SQLException {
		Map<java.sql.Date, Integer> diasConMenosViajes = new HashMap<Date, Integer>();
		while (topNDiasMenosViajesRs.next()) {
			diasConMenosViajes.put(topNDiasMenosViajesRs.getDate(1), topNDiasMenosViajesRs.getInt(2));
		}
		return diasConMenosViajes;
	}

	/**
	 * Construye un String separado por comas con 'N' placeholders
	 * 
	 * @param size
	 * @return String CSV
	 */
	private static String buildPlaceholderCSV(int size) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < size; i++) {
			builder.append("?,");
		}
		builder.deleteCharAt(builder.length() - 1);
		return builder.toString();
	}
	
}
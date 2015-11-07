package middleware.core;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import middleware.db.MySqlDBConnectionFactory;

public class MiddlewareJP {
	
	/**
	 * El valor que representa un error en la medicion de temperatura, por parte de las estaciones de monitoreo.
	 */
	private static final BigDecimal VALUE_ERROR_MEASURING = BigDecimal.valueOf(1802.6);
	
	
	/**
	 * Indica la fecha de comienzo del periodo sobre el cual trabajeremos. Los primeros 3 meses se descartan por no poseer valores significativos.
	 * Esto se debe a que en esos meses, mucha gente sale de la ciudad por sus vacaciones de verano.  
	 */
	private static final String START_PERIOD = "2011-04-01";
	
	
	
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
    	// TODO: pasar por parametro;
        int topN = 5;
           
        // Obtenemos las conexiones de las Bases de Datos: Meteorologia y Bicicletas
        Connection meteorologiaDBConn = MySqlDBConnectionFactory.METEOROLOGIA_DB.getConnection();
        Connection bicicletasDBConn = MySqlDBConnectionFactory.BICICLETAS_DB.getConnection();
       
        PreparedStatement preparedStatement = null;
        ResultSet dataMeteorologicaRs = null ;
        ResultSet topNDiasMenosViajesRs = null;
		try {
			// Obtenemos los 'N' dias en los que hubo 
			// menos viajes en bicicleta en la ciudad sobre la base de datos Bicicletas
			preparedStatement = buildQueryDiasMenosViajesTopN(topN, bicicletasDBConn);
	        topNDiasMenosViajesRs = preparedStatement.executeQuery();
	        
	        // Construimos un mapa con los resultados
			Map<java.sql.Date, Integer> diasConMenosViajes = getDiasConMenosViajes(topNDiasMenosViajesRs); 
			
			// Obtenemos la informacion meteorologica para los dias 
			// en los que menos viajes en bicicleta hubo en la ciudad, sobre la base de datos Meteorologia
			preparedStatement = buildQueryDataMeteorologia(meteorologiaDBConn, diasConMenosViajes);
			dataMeteorologicaRs = preparedStatement.executeQuery();
			
			printResults(dataMeteorologicaRs, diasConMenosViajes); 
	        
		} catch (SQLException e) {
			System.err.println("Un error ha ocurrido al intentar acceder a los datos");
		} catch (ParseException e) {
			System.err.println("Un error ha ocurrido al intentar dar formato a la fecha " + DATE_FORMATTER);
		} finally {
			closeResourcesDB(meteorologiaDBConn, bicicletasDBConn, preparedStatement, dataMeteorologicaRs,
					topNDiasMenosViajesRs);
		}
    }


	/**
	 * @param connection
	 * @param diasConMenosViajes
	 * @return
	 * @throws SQLException
	 */
	private static PreparedStatement buildQueryDataMeteorologia(Connection connection,
			Map<java.sql.Date, Integer> diasConMenosViajes) throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(
				  "select fecha, min(temp_c) as temp_min, max(temp_c) as temp_max, avg(temp_c) temp_promedio, max(pluv_mm) as lluvia"
//				"select fecha, avg(temp_c) temp_promedio, max(pluv_mm) lluviaMm"
				+ " from info_metereologica" 
				+ " where temp_c <> ?"		  
				+ " and fecha in (" + buildPlaceholderCSV(diasConMenosViajes.size()) + ")"
		        + " group by fecha"
				+ " order by fecha;");
		preparedStatement.setBigDecimal(1, VALUE_ERROR_MEASURING);
		
		int index = 2;
		for (Map.Entry<java.sql.Date, Integer> entry : diasConMenosViajes.entrySet()) {
			preparedStatement.setDate(index++, entry.getKey());
		}
		
		return preparedStatement;
	}

	/**
	 * @param topN
	 * @param connection
	 * @return
	 * @throws SQLException
	 * @throws ParseException 
	 */
	private static PreparedStatement buildQueryDiasMenosViajesTopN(int topN, Connection connection)
			throws SQLException, ParseException {
		PreparedStatement preparedStatement = connection.prepareStatement(
				  " select DATE(origenFecha) fecha, count(*) as cantViajes from viajes_bicicletas"
				+ " where DATE(origenFecha) > ?"  
				+ " group by fecha"
				+ " order by cantViajes asc"
		     + " limit " + topN + ";");
		
		preparedStatement.setDate(1, new java.sql.Date(DATE_FORMATTER.parse(START_PERIOD).getTime()));
		
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
	private static void printResults(ResultSet dataMeteorologicaRs, Map<Date, Integer> diasConMenosViajes) throws SQLException {
		while (dataMeteorologicaRs.next()) {
			Date fecha = dataMeteorologicaRs.getDate(1);
			
			System.out.println("Fecha: " + fecha
			+ "	Cant. Viajes: " + diasConMenosViajes.get(fecha) + "	"
			+ "	Temp. Min: " + DECIMAL_FORMATTER.format(dataMeteorologicaRs.getDouble(2)) + "	°C"
			+ "	Temp. Max: " + DECIMAL_FORMATTER.format(dataMeteorologicaRs.getDouble(3)) + "	°C"
			+ "	Temp. Media: " + DECIMAL_FORMATTER.format(dataMeteorologicaRs.getDouble(4)) + "	°C"
			+ "	Cant. Lluvia: " + dataMeteorologicaRs.getShort(5) + "	mm"
					);	
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
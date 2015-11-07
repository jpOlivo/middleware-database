package middleware.core;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import middleware.db.MySqlDBConnectionFactory;

public class MiddlewareJP2 {
	
	private static final Short INDICATOR_DRY_DAY = Short.valueOf("0");


	private static final Short INDICATOR_RAINY_DAY = Short.valueOf("50");


	private static final Integer INDICATOR_WARM_DAY = Integer.valueOf(30);


	/**
	 * El valor que representa un error en la medicion de temperatura, por parte de las estaciones de monitoreo.
	 */
	private static final BigDecimal VALUE_ERROR_MEASURING = BigDecimal.valueOf(1802.6);
	
	private static final DecimalFormat DECIMAL_FORMATTER = new DecimalFormat("#.0"); 
	
	
	
	
    /**
     * El ingreso de autos a la ciudad aumenta o disminuye los dias de lluvia? en que porcentaje? 
     * 
     * @param args the command line arguments
     */
    public static void main(String[] args) {
		Connection meteorologiaDBConn = null, vehiculosDBConn = null;
		try {
			// Obtenemos las conexiones de las Bases de Datos: Meteorologia y Bicicletas de la ciudad
			meteorologiaDBConn = MySqlDBConnectionFactory.METEOROLOGIA_DB.getConnection();
			vehiculosDBConn = MySqlDBConnectionFactory.VEHICULOS_DB.getConnection();

			double promedioIngresosDiaLluviaOrCalido = getPromedioIngresosDiasCalidosOrLLuvia(vehiculosDBConn, meteorologiaDBConn);
			double promedioIngresosDiaNormal = getPromedioIngresosDiasNormales(vehiculosDBConn, meteorologiaDBConn);
			double difPorcentaje = (promedioIngresosDiaLluviaOrCalido - promedioIngresosDiaNormal) * 100 / (promedioIngresosDiaLluviaOrCalido + promedioIngresosDiaNormal);
			
			String text = difPorcentaje > 0 ? "mas" : "menos";
			System.out.println("Los dias de lluvia o mas calidos, ingresa a la ciudad un " + DECIMAL_FORMATTER.format(Math.abs(difPorcentaje)) + "% de vehiculos " + text);
			
		} finally {
			closesDBResources(meteorologiaDBConn, vehiculosDBConn);
		}
	}

	private static void closesDBResources(Connection meteorologiaDBConn, Connection vehiculosDBConn) {
		try {
			meteorologiaDBConn.close();
			vehiculosDBConn.close();
		} 	catch (Exception e) {
			System.err.println("Un error ha ocurrido al intentar cerrar las DB");
		}
	}
    
    private static double getPromedioIngresosDiasNormales(
			Connection vehiculosDBConn, Connection meteorologiaDBConn) {
    	double promedioIngresosDia = 0;
    	PreparedStatement preparedStatement = null;
		ResultSet diasRs = null, ingresosRs = null;
		
		try {
		// Obtenemos de la Base de Datos Meteorologia, los dias en los que NO se registraron lluvias y que la temperatura fue menor a 30°C		
		preparedStatement = buildQueryDiasNoLluvia(meteorologiaDBConn);
		diasRs = preparedStatement.executeQuery();
		
		// Construimos un mapa con los resultados
		Map<java.sql.Date, Integer> diasMap = getDiasMap(diasRs); 
					
		// Obtenemos de la Base de Datos Transito, la cantidad de vehiculos que ingresaron a la ciudad en aquellos dias donde no hubo lluvias y la temperatura fue menor a 30°C
		preparedStatement = buildQueryIngresosDia(vehiculosDBConn, diasMap);
		ingresosRs = preparedStatement.executeQuery();
					
		// Calculamos la media de ingresos por dia
		promedioIngresosDia = getPromedioAutosDia(ingresosRs);
		System.out.println("Promedio vehiculos ingresan ciudad dia menos de 30°C y sin lluvia:	" + promedioIngresosDia);
		
		} catch (Exception e) {
			System.err.println("Ha oceurrido un error al intentar calcular el promedio de ingresos para los dias normales");
		} finally {
			closeDBResources(preparedStatement, diasRs, ingresosRs);
		}
		return promedioIngresosDia;
	}

	private static double getPromedioIngresosDiasCalidosOrLLuvia(Connection vehiculosDBConn, Connection meteorologiaDBConn) {
		double promedioIngresosLluviaOrCalido = 0;
		PreparedStatement preparedStatement = null;
		ResultSet diasRs = null, ingresosRs = null;
		try {
			// Obtenemos de la Base de Datos Meteorologia, los dias calidos o lluviosos	
			preparedStatement = buildQueryDiasLluviaOrCalido(meteorologiaDBConn);
			diasRs = preparedStatement.executeQuery();
	        
	        // Construimos un mapa con los resultados
			Map<java.sql.Date, Integer> diasMap = getDiasMap(diasRs); 
			
			// Obtenemos de la Base de Datos Transito, la cantidad de vehiculos que ingresaron a la ciudad aquellos dias de lluvia o calidos 
			preparedStatement = buildQueryIngresosDia(vehiculosDBConn, diasMap);
			ingresosRs = preparedStatement.executeQuery();
			
			// Calculamos la media de ingresos por dia 
			promedioIngresosLluviaOrCalido = getPromedioAutosDia(ingresosRs);
			System.out.println("Promedio vehiculos ingresan ciudad dia de lluvia o dia de mas de 30°C:	" + promedioIngresosLluviaOrCalido);
			
			
		} catch (Exception e) {
			System.err.println("Ha oceurrido un error al intentar calcular el promedio de ingresos para los dias de lluvia o calidos");
		} finally {
			closeDBResources(preparedStatement, diasRs, ingresosRs);
		}
		return promedioIngresosLluviaOrCalido;
    }

	private static void closeDBResources(PreparedStatement preparedStatement, ResultSet diasRs, ResultSet ingresosRs) {
		try {
			diasRs.close();
			ingresosRs.close();
			preparedStatement.close();
		} catch (Exception e) {
			System.err.println("Un error ha ocurrido al intentar cerrar los recursos de DB");
		}
	}


	private static double getPromedioAutosDia(ResultSet ingresosPorDia) throws SQLException {
		int ingresosTotal = 0;
		int i = 0;
		while (ingresosPorDia.next()) {
			ingresosTotal = ingresosTotal + ingresosPorDia.getInt(2);
			i++;
		}
		return ingresosTotal / i;
	}


	/**
	 * @param meteorologiaDBConn
	 * @param diasConMenosViajes
	 * @return
	 * @throws SQLException
	 */
	private static PreparedStatement buildQueryIngresosDia(Connection connection,
			Map<java.sql.Date, Integer> diasMap) throws SQLException {
		
		PreparedStatement preparedStatement = connection.prepareStatement(
				  " select fecha, sum(cantPasos) from flujo_vehicular"
				+ " where fecha in (" + buildPlaceholderCSV(diasMap.size()) + ")"
		        + " group by fecha;");
		
		int index = 1;
		for (Map.Entry<java.sql.Date, Integer> entry : diasMap.entrySet()) {
			preparedStatement.setDate(index++, entry.getKey());
		}
		
		return preparedStatement;
	}
	
	/**
	 * @param connection
	 * @return
	 * @throws SQLException
	 * @throws ParseException
	 */
	private static PreparedStatement buildQueryDiasLluviaOrCalido(Connection connection)
			throws SQLException, ParseException {
		
		PreparedStatement preparedStatement = connection.prepareStatement(
				  " select fecha, max(temp_c), max(pluv_mm) from info_metereologica"
				+ " where (temp_c <> ? and temp_c > ?) or pluv_mm > ?"  
				+ " group by fecha"
				+ " order by fecha;");
		preparedStatement.setBigDecimal(1, VALUE_ERROR_MEASURING);
		preparedStatement.setInt(2, INDICATOR_WARM_DAY);
		preparedStatement.setShort(3, INDICATOR_RAINY_DAY);
		
		return preparedStatement;
	}
	
	
	/**
	 * @param connection
	 * @return
	 * @throws SQLException
	 * @throws ParseException
	 */
	private static PreparedStatement buildQueryDiasNoLluvia(Connection connection)
			throws SQLException, ParseException {
		
//		select fecha, max(pluv_mm) from info_metereologica 
//		where pluv_mm = 0
//		group by fecha
//		order by fecha;
		
//		PreparedStatement preparedStatement = connection.prepareStatement(
//				  " select fecha, max(pluv_mm) from info_metereologica"
//				+ " where pluv_mm = ?"  
//				+ " group by fecha"
//				+ " order by fecha;");
//		preparedStatement.setShort(1, Short.valueOf("0"));
//	
//		return preparedStatement;
		
		PreparedStatement preparedStatement = connection.prepareStatement(
				  " select fecha, max(temp_c), pluv_mm from info_metereologica"
				+ " where temp_c < ? and pluv_mm = ?"  
				+ " group by fecha"
				+ " order by fecha;");
		preparedStatement.setInt(1, INDICATOR_WARM_DAY);
		preparedStatement.setShort(2, INDICATOR_DRY_DAY);
		
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


	private static Map<java.sql.Date, Integer> getDiasMap(ResultSet resultSet)
			throws SQLException {
		Map<java.sql.Date, Integer> diasMap = new HashMap<Date, Integer>();
		while (resultSet.next()) {
			diasMap.put(resultSet.getDate(1), resultSet.getInt(2));
		}
		return diasMap;
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
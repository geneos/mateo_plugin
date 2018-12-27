package org.openXpertya.grid;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import javax.swing.JOptionPane;

import org.openXpertya.model.MInOut;
import org.openXpertya.model.MInOutLine;
import org.openXpertya.model.MInvoice;
import org.openXpertya.model.MInvoiceLine;
import org.openXpertya.model.MLocator;
import org.openXpertya.model.MMatchPO;
import org.openXpertya.model.MOrder;
import org.openXpertya.model.MOrderLine;
import org.openXpertya.model.MProduct;
import org.openXpertya.model.MStorage;
import org.openXpertya.model.X_C_DocType;
import org.openXpertya.util.CLogger;
import org.openXpertya.util.DB;
import org.openXpertya.util.Env;
import org.openXpertya.util.Util;

import ar.com.geneos.mrp.plugin.util.MUMStorage;

public class CreateFromShipmentModel extends CreateFromModel {

	// =============================================================================================
	// Logica en comun para la carga de facturas
	// =============================================================================================

	/**
	 * Consulta para carga de facturas
	 */
	public StringBuffer loadInvoiceQuery() {
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT ")
		// Entered UOM
		.append("l.C_InvoiceLine_ID, ")
		.append("l.Line, ")
		.append("l.Description, ")
		.append("l.M_Product_ID, ")
		.append("p.Name AS ProductName, ")
		.append("p.value AS ItemCode, ")
		.append("p.producttype AS ProductType, ")
		.append("l.C_UOM_ID, ")
		.append("QtyInvoiced, ")
		.append("l.QtyInvoiced-SUM(NVL(mi.Qty,0)) AS RemainingQty, ")
		.append("l.QtyEntered/l.QtyInvoiced AS Multiplier, ")
		.append("COALESCE(l.C_OrderLine_ID,0) AS C_OrderLine_ID, ")
		.append("l.M_AttributeSetInstance_ID AS AttributeSetInstance_ID ")

		.append("FROM C_UOM uom, C_InvoiceLine l, M_Product p, M_MatchInv mi ")

		.append("WHERE l.C_UOM_ID=uom.C_UOM_ID ")
		.append("AND l.M_Product_ID=p.M_Product_ID ")
		.append("AND l.C_InvoiceLine_ID=mi.C_InvoiceLine_ID(+) ")
		.append("AND l.C_Invoice_ID=? ")
		.append("GROUP BY l.QtyInvoiced, l.QtyEntered/l.QtyInvoiced, l.C_UOM_ID, l.M_Product_ID, p.Name, l.C_InvoiceLine_ID, l.Line, l.C_OrderLine_ID, l.Description, p.value,l.M_AttributeSetInstance_ID,p.producttype ")
		.append("ORDER BY l.Line ");
		return sql;
	}
	
	public void loadInvoiceLine(InvoiceLine invoiceLine, ResultSet rs) throws SQLException {

		// Por defecto no está seleccionada para ser procesada
		invoiceLine.selected = false;

		// ID de la línea de factura
		invoiceLine.invoiceLineID = rs.getInt("C_InvoiceLine_ID");

		// Nro de línea
		invoiceLine.lineNo = rs.getInt("Line");

		// Descripción
		invoiceLine.description = rs.getString("Description");

		// Cantidades
		BigDecimal multiplier = rs.getBigDecimal("Multiplier");
		BigDecimal qtyInvoiced = rs.getBigDecimal("QtyInvoiced")
				.multiply(multiplier);
		BigDecimal remainingQty = rs.getBigDecimal("RemainingQty")
				.multiply(multiplier);
		invoiceLine.lineQty = qtyInvoiced;
		invoiceLine.remainingQty = remainingQty;

		// Artículo
		invoiceLine.productID = rs.getInt("M_Product_ID");
		invoiceLine.productName = rs.getString("ProductName");
		invoiceLine.itemCode = rs.getString("ItemCode");
		invoiceLine.instanceName = getInstanceName(rs
				.getInt("AttributeSetInstance_ID"));
		invoiceLine.productType = rs.getString("ProductType");

		// Unidad de Medida
		invoiceLine.uomID = rs.getInt("C_UOM_ID");
		invoiceLine.uomName = getUOMName(invoiceLine.uomID);

		// Línea de pedido (puede ser 0)
		invoiceLine.orderLineID = rs.getInt("C_OrderLine_ID");
	}
	

	public String getRemainingQtySQLLine(MInOut inout, boolean forInvoice, boolean allowDeliveryReturns){
		boolean afterInvoicing = (inout.getDeliveryRule().equals(
				MInOut.DELIVERYRULE_AfterInvoicing) || inout.getDeliveryRule()
				.equals(MInOut.DELIVERYRULE_Force_AfterInvoicing))
				&& inout.getMovementType().endsWith("-");
		String srcColumn = afterInvoicing ? "l.QtyInvoiced" : "l.QtyOrdered";
		return srcColumn
				+ " - (l.QtyDelivered+l.QtyTransferred)"
				+ (allowDeliveryReturns ? ""
						: " - coalesce(("+MInOut.getNotAllowedQtyReturnedSumQuery()+"),0)");
	}
	
	/**
	 * Efectiviza la persistencia los POs
	 */
	public void save(Integer locatorID, MInOut inOut, MOrder p_order, MInvoice m_invoice, List<? extends SourceEntity> selectedSourceEntities, String trxName, boolean isSOTrx, CreateFromPluginInterface handler) throws CreateFromSaveException {
		// La ubicación es obligatoria
		if (locatorID == null || (locatorID == 0)) {
//			locatorField.setBackground(CompierePLAF.getFieldBackground_Error());
			throw new CreateFromSaveException("@NoLocator@");
		}

		// Actualiza el encabezado del remito (necesario para validaciones en
		// las
		// líneas a crear del remito)
		MInOut inout = inOut;
		log.config(inout + ", C_Locator_ID=" + locatorID);
		// Asocia el pedido
		if (p_order != null) {
			inout.setC_Order_ID(p_order.getC_Order_ID());
			inout.setDateOrdered(p_order.getDateOrdered());
			inout.setC_Project_ID(p_order.getC_Project_ID());
			inout.setPOReference(p_order.getPOReference());
			// Si no se debe utilizar el depósito del pedido, entonces tampoco
			// su organización sino hay una inconsistencia de los datos
			if(isUseOrderWarehouse(inout, trxName)){
				inout.setAD_Org_ID(p_order.getAD_Org_ID());
			}
			inout.setAD_OrgTrx_ID(p_order.getAD_OrgTrx_ID());
			inout.setC_Campaign_ID(p_order.getC_Campaign_ID());
			inout.setUser1_ID(p_order.getUser1_ID());
			inout.setUser2_ID(p_order.getUser2_ID());
			setWarehouse(inout, p_order, trxName);
			inout.setDeliveryRule(p_order.getDeliveryRule());
			inout.setDeliveryViaRule(p_order.getDeliveryViaRule());
			inout.setM_Shipper_ID(p_order.getM_Shipper_ID());
			inout.setFreightCostRule(p_order.getFreightCostRule());
			inout.setFreightAmt(p_order.getFreightAmt());
			inout.setC_BPartner_ID(p_order.getBill_BPartner_ID());
		}
		// Asocia la factura
		if ((m_invoice != null) && (m_invoice.getC_Invoice_ID() != 0)) {
			inout.setC_Invoice_ID(m_invoice.getC_Invoice_ID());
		}

		// Guarda el encabezado. Si hay error cancela la operación
		if (!inout.save()) {
			throw new CreateFromSaveException(CLogger.retrieveErrorAsString());
		}

		// Lines
		Integer productLocatorID = null;
		MLocator productLocator = null;
		boolean noStock = false;
		String msg = "Los siguientes articulos no tienen stock suficiente: \n";
		
		//Control por si se repite el producto en la linea del pedido
		//Para acumuar el stock utilizado
		BigDecimal usedQty = BigDecimal.ZERO;
		int last_Product_ID = 0;
		for (SourceEntity sourceEntity : selectedSourceEntities) {
			DocumentLine docLine = (DocumentLine) sourceEntity;
			BigDecimal movementQty = docLine.remainingQty;
			int C_UOM_ID = docLine.uomID;
			int M_Product_ID = docLine.productID;
			
			// Determinar la ubicación relacionada al artículo y verificar que
			// se encuentre dentro del almacén del remito. Si se encuentra en
			// este almacén, entonces setearle la ubicación del artículo, sino
			// la ubicación por defecto. Sólo para movimientos de ventas.
			productLocatorID = null;
			if(isSOTrx){
				// Obtengo el id de la ubicación del artículo
				productLocatorID = MProduct.getLocatorID(M_Product_ID, trxName);
				// Si posee una configurada, verifico que sea del mismo almacén,
				// sino seteo a null el id de la ubicación para que setee el que
				// viene por defecto
				if(!Util.isEmpty(productLocatorID, true)){
					productLocator = MLocator.get(ctx, productLocatorID);
					productLocatorID = productLocator.getM_Warehouse_ID() != inout
							.getM_Warehouse_ID() ? null : productLocatorID;
				}
			}
			
			/* 
			 * GENEOS - Explosion de cantidades en las distintas partidas existentes segun FiFo
			 * SOLO PARA REMITOS DE SALIDA
			 */
			if (inOut.isSOTrx() && inOut.getMovementType().endsWith("-")) {
				MProduct product = MProduct.get(ctx, M_Product_ID);
				int M_Locator_ID = locatorID;
				BigDecimal outQty = BigDecimal.ZERO;
				// Solo si tiene conjunto de atributos definido
				if (product.getM_AttributeSet_ID() != 0) {
					MStorage[] storages = MUMStorage.getOfProduct(ctx, M_Product_ID, M_Locator_ID, product.getM_AttributeSet_ID(), true, true, trxName);
					
					//Reseteo cantidad usada cuando cambio de articulo
					if (M_Product_ID != last_Product_ID) 
						usedQty = BigDecimal.ZERO;
					
					for (MStorage storage : storages){
						
						
						
						//Si tengo cantidad usada entonces salteo storages
						if (usedQty.signum() == 1){
							if (storage.getQtyOnHand().compareTo(usedQty) <= 0){
								usedQty = usedQty.subtract(storage.getQtyOnHand());
								continue;
							}
							else
								//Modifico de manera temporaria la cantidad en mano restando lo utilizado
								storage.setQtyOnHand( storage.getQtyOnHand().subtract(usedQty) );
						}
							
							
						outQty = movementQty;
						if (storage.getQtyOnHand().compareTo(outQty) <= 0){
							outQty = storage.getQtyOnHand();
						}
						// Crea la línea del remito
						createInOutLine(inout,m_invoice,docLine,outQty,M_Locator_ID,storage.getM_AttributeSetInstance_ID(),handler,trxName);

						movementQty = movementQty.subtract(outQty);
						usedQty = usedQty.add(outQty);
						if (movementQty.signum() <= 0){
							last_Product_ID = M_Product_ID;
							break;
						}
					}
				
					// No alcanzo el stock
					if (movementQty.signum() ==1 ) {
						last_Product_ID = M_Product_ID;
						noStock = true;
						msg += product.getName()+" faltan "+movementQty+"\n";
					}
				} else {
					createInOutLine(inout,m_invoice,docLine,movementQty,M_Locator_ID,0,handler,trxName);
				}
			} else {
				
				/* 
				 * GENEOS - Explosion de cantidades en las distintas partidas entregadas al cliente
				 * SOLO PARA REMITOS DE SALIDA -> Devolucion de clientes
				 * CREADAS DESDE UN PEDIDO DE CLIENTE
				 */
				if (inOut.isSOTrx() && inOut.getMovementType().endsWith("+") && docLine.isOrderLine()) {
					MProduct product = MProduct.get(ctx, M_Product_ID);
					System.out.println("Devolucion!");

					// Solo si tiene conjunto de atributos definido
					if (product.getM_AttributeSet_ID() != 0) {
						
						//Obtengo lineas entregadas para ese pedido
						OrderLine orderLine = (OrderLine) docLine;
						String where = " M_InOut_ID IN (SELECT M_InOut_ID from M_InOut where movementtype = 'C-' AND DocStatus in ('CO','CL') )";
						
						MInOutLine[] lines = MInOutLine.getOfOrderLine(ctx, orderLine.orderLineID, where, trxName);
												
						for (MInOutLine ioLine : lines){
							BigDecimal qtyToReturn = ioLine.getMovementQty();
							//Verifico devoluciones ya realizadas sobre esta linea
							where = " M_AttributeSetInstance_ID = "+ioLine.getM_AttributeSetInstance_ID()+" AND M_InOut_ID IN (SELECT M_InOut_ID from M_InOut where movementtype = 'C+' AND DocStatus in ('CO','CL'))";
							MInOutLine[] linesReturns = MInOutLine.getOfOrderLine(ctx, orderLine.orderLineID, where, trxName);
							for (MInOutLine ioLineReturn : linesReturns){
								qtyToReturn = qtyToReturn.subtract(ioLineReturn.getMovementQty());
							}
							if (qtyToReturn.signum() == 1)
								// Crea la línea del remito
								createInOutLine(inout,m_invoice,docLine,qtyToReturn,ioLine.getM_Locator_ID(),ioLine.getM_AttributeSetInstance_ID(),handler,trxName);
						}
					
					} else {
						createInOutLine(inout,m_invoice,docLine,movementQty,locatorID,0,handler,trxName);
					}
				} else {
					createInOutLine(inout,m_invoice,docLine,movementQty,locatorID,0,handler,trxName);
				}
			}
		
		} // for all rows

		if (noStock) {
			msg += "\n¿Desea continuar de todas maneras?";
			int rta = JOptionPane.showConfirmDialog(null, msg, "Falta stock", JOptionPane.YES_NO_OPTION);
			if (rta != 0){
				throw new CreateFromSaveException("Operación cancelada");
			}
		}
	} // save
	
	
	private void createInOutLine(MInOut inout, MInvoice m_invoice, DocumentLine docLine, BigDecimal movementQty, int M_Locator_ID,int M_AttributeSetInstance_ID, CreateFromPluginInterface handler,String trxName) throws CreateFromSaveException {
		// Crea la línea del remito
		
		MInOutLine iol = new MInOutLine(inout);
		iol.setM_Product_ID(docLine.getProductID(), docLine.getUomID()); // Line UOM
		iol.setQty(movementQty); // Movement/Entered
		iol.setM_Locator_ID(M_Locator_ID); // Locator
		iol.setDescription(docLine.description);

		MInvoiceLine il = null;
		MOrderLine ol = null;

		// La línea del remito se crea a partir de una línea de pedido
		if (docLine.isOrderLine()) {
			OrderLine orderLine = (OrderLine) docLine;
			// Asocia línea remito -> línea pedido
			iol.setC_OrderLine_ID(orderLine.orderLineID);
			ol = new MOrderLine(Env.getCtx(), orderLine.orderLineID,
					trxName);
			// Proyecto
			iol.setC_Project_ID(ol.getC_Project_ID());
			if (ol.getQtyEntered().compareTo(ol.getQtyOrdered()) != 0) {
				iol.setMovementQty(movementQty.multiply(ol.getQtyOrdered())
						.divide(ol.getQtyEntered(),
								BigDecimal.ROUND_HALF_UP));
				iol.setC_UOM_ID(ol.getC_UOM_ID());
			}

			// Instancia de atributo
			if (M_AttributeSetInstance_ID == 0) {
				if (ol.getM_AttributeSetInstance_ID() != 0) {
					iol.setM_AttributeSetInstance_ID(ol
							.getM_AttributeSetInstance_ID());
				}
			}
			else
				iol.setM_AttributeSetInstance_ID(M_AttributeSetInstance_ID);
			
			// Cargo (si no existe el artículo)
			if (docLine.getProductID() == 0 && ol.getC_Charge_ID() != 0) {
				iol.setC_Charge_ID(ol.getC_Charge_ID());
			}
			
			// Este metodo es redefinido por un plugin
			handler.customMethod(ol,iol);

			// La línea del remito se crea a partir de una línea de factura
		} else if (docLine.isInvoiceLine()) {
			InvoiceLine invoiceLine = (InvoiceLine) docLine;
			// Credit Memo - negative Qty
			if (m_invoice != null && m_invoice.isCreditMemo()) {
				movementQty = movementQty.negate();
			}
			il = new MInvoiceLine(Env.getCtx(), invoiceLine.invoiceLineID,
					trxName);
			// Proyecto
			iol.setC_Project_ID(il.getC_Project_ID());
			if (il.getQtyEntered().compareTo(il.getQtyInvoiced()) != 0) {
				iol.setQtyEntered(movementQty.multiply(il.getQtyInvoiced())
						.divide(il.getQtyEntered(),
								BigDecimal.ROUND_HALF_UP));
				iol.setC_UOM_ID(il.getC_UOM_ID());
			}
			// Instancia de atributo
			if (M_AttributeSetInstance_ID == 0) {
				if (il.getM_AttributeSetInstance_ID() != 0) {
					iol.setM_AttributeSetInstance_ID(il
							.getM_AttributeSetInstance_ID());
				}
			}
			else
				iol.setM_AttributeSetInstance_ID(M_AttributeSetInstance_ID);
			
			// Cargo (si no existe el artículo)
			if (docLine.getProductID() == 0 && il.getC_Charge_ID() != 0) {
				iol.setC_Charge_ID(il.getC_Charge_ID());
			}
			// Si la línea de factura estaba relacionada con una línea de
			// pedido
			// entonces se hace la asociación a la línea del remito. Esto es
			// necesario
			// para que se actualicen los valores QtyOrdered y QtyReserved
			// en el Storage
			// a la hora de completar el remito.
			if (invoiceLine.orderLineID > 0) {
				iol.setC_OrderLine_ID(invoiceLine.orderLineID);
			}
			iol.setC_InvoiceLine_ID(invoiceLine.invoiceLineID);
		}

		// Guarda la línea de remito
		if (!iol.save()) {
			throw new CreateFromSaveException("@InOutLineSaveError@ (# "
					+ docLine.lineNo + "):<br>"
					+ CLogger.retrieveErrorAsString());

			// Create Invoice Line Link
		} else if (il != null) {
			il.setM_InOutLine_ID(iol.getM_InOutLine_ID());
			if (!il.save()) {
				throw new CreateFromSaveException(
						"@InvoiceLineSaveError@ (# " + il.getLine()
								+ "):<br>"
								+ CLogger.retrieveErrorAsString());
			}
		}
		
	}

	public boolean beforeAddOrderLine(OrderLine orderLine, MInOut inOut, boolean isSOTrx) {
		// Si la línea de pedido ya está asociada con alguna línea del remito
		// entonces
		// no debe ser mostrada en la grilla. No se permite que dos líneas de un
		// mismo
		// remito compartan una línea del pedido.
		String sql = "SELECT COUNT(*) FROM M_InOutLine WHERE M_InOut_ID = ? AND C_OrderLine_ID = ?";
		Long count = (Long) DB.getSQLObject(null, sql, new Object[] {
				inOut.getM_InOut_ID(), orderLine.orderLineID });
		if (count != null && count > 0) {
			return false;
		}

		// Para devoluciones de clientes, la cantidad pendiente es en realidad
		// la cantidad que se le ha entregado.
		if ((isSOTrx && inOut.getMovementType().endsWith("+"))
				|| (!isSOTrx && inOut.getMovementType().endsWith("-"))) {
			orderLine.remainingQty = orderLine.qtyDelivered;
		}

		return true;
	}
	
	/*
	 * Setea el warehouse del remito a partir del especificado en el pedido
	 * UNICAMENTE si la configuración del tipo de documento así lo especifica,
	 * o bien si todavía este dato no se encuentra especificado
	 */
	protected void setWarehouse(MInOut inOut, MOrder order, String trxName) {
		// Si no esta seteado o bien hay que forzar el warehouse del pedido, setearlo
		if ((inOut.getM_Warehouse_ID() <= 0) || isUseOrderWarehouse(inOut, trxName)) {
			inOut.setM_Warehouse_ID(order.getM_Warehouse_ID());
		}
	}
	
	/**
	 * @param inOut
	 * @param trxName
	 * @return true si el tipo de documento del remito parámetro está marcado para que
	 *         copie el almacén del pedido
	 */
	protected boolean isUseOrderWarehouse(MInOut inOut, String trxName){
		// Recuperar tipo de documento
		X_C_DocType inOutDocType = new X_C_DocType(ctx, inOut.getC_DocType_ID(), trxName);
		return inOutDocType.isUseOrderWarehouse();
	}
	
}

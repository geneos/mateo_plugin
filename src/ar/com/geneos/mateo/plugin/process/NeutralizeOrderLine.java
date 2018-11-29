package ar.com.geneos.mateo.plugin.process;

import java.math.BigDecimal;

import org.openXpertya.model.MOrder;
import org.openXpertya.model.MOrderLine;
import org.openXpertya.model.X_C_Order;
import org.openXpertya.process.SvrProcess;
import org.openXpertya.util.CLogger;


public class NeutralizeOrderLine extends SvrProcess {
	
	MOrderLine line;
	MOrder order;
	
	@Override
	protected void prepare() {
		line = new MOrderLine(getCtx(), getRecord_ID(), get_TrxName());
		order = line.getOrder();
	}

	@Override
	protected String doIt() throws Exception {
		checkBussinesRules();
		
		String m_processMsg = "";		
		BigDecimal old  = line.getQtyOrdered();
		BigDecimal qtyDelivered = line.getQtyDelivered().add(line.getQtyTransferred());
		
        if( old.compareTo(qtyDelivered) != 0 ) {
        	line.setQtyOrdered(qtyDelivered);
        	line.addDescription( "Neutralizado (" + old + ")" );
        	if(!line.save( get_TrxName())){
        		m_processMsg = CLogger.retrieveErrorAsString();
        		throw new Exception(m_processMsg);
            }		
        }
        
        int[] orderLines = {getRecord_ID()};
        if( !order.reserveStock( null, orderLines )) {
        	m_processMsg = CLogger.retrieveErrorAsString();
    		throw new Exception(m_processMsg);
        }
        return "OK";
	}
	
	private void checkBussinesRules() throws Exception {
		if(order == null || !order.getDocStatus().equals(X_C_Order.DOCSTATUS_Completed)){
			throw new Exception("El pedido debe estar completo para utilizar esta funcionalidad");
		}
	}
}

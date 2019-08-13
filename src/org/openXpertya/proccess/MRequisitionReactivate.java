package org.openXpertya.proccess;

import org.openXpertya.model.MRequisition;
import org.openXpertya.process.DocAction;
import org.openXpertya.process.SvrProcess;

public class MRequisitionReactivate extends SvrProcess {

	private int m_requisition_id = 0;

	public MRequisitionReactivate() {
	}

	@Override
	protected void prepare() {
		if (this.m_requisition_id < 1) {
			m_requisition_id = super.getRecord_ID();
		}
	}

	@Override
	protected String doIt() throws Exception {

		// MRequsition
		MRequisition req = new MRequisition(getCtx(), this.m_requisition_id, get_TrxName());
		req.processIt(DocAction.ACTION_ReActivate);
		if (!req.save())
			return "Error";

		return "Ok";
	}

}

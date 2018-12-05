package org.openXpertya.proccess;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import org.openXpertya.model.MAttribute;
import org.openXpertya.model.MAttributeSet;
import org.openXpertya.model.MAttributeUse;
import org.openXpertya.model.MProduct;
import org.openXpertya.model.PO;
import org.openXpertya.model.Query;
import org.openXpertya.process.DocAction;
import org.openXpertya.process.ProcessInfoParameter;
import org.openXpertya.process.SvrProcess;
import org.openXpertya.util.CLogger;

public class SetCustomMAttribute extends SvrProcess {

	private String prodNotFoundMsg = "";
	private String prodAttrNotFoundMsg = "";
	
	private ArrayList<MProduct> products = new ArrayList<MProduct>();
	private int customAttribute_ID = 0;
	
	String customAttributeName =  "Lote Proveedor";
	String[] producValues = 
		{"Acido Sulfúrico 98%"
,"Caja 100 N Venado BCI 31 G-31 Caterpiller"
,"Caja 110 Baja N Soyuz L5"
,"Caja 110 N (12x80 Venado)"
,"Caja 120 N (12x100 Venado)"
,"Caja 160 Baja N Venado CC"
,"Caja 160 N Venado CC"
,"Caja 160 N Venado SC"
,"Caja 200 N Venado"
,"Caja 50 Baja N SOYUZ (L1)"
,"Caja 50 N Soyuz (L1B)"
,"Caja 65 N Soyuz / Venado"
,"Caja 75 Baja N Soyuz L3 0mm"
,"Caja 75 N Soyuz L3B 3mm"
,"Caja 80 N (12x75 Alta Venado)"
,"Caja 90 B Voss2000"
,"Calcio Aluminio"
,"Cubre Tapon 12x160 LM Venado"
,"cubre tapon 12x80"
,"Cubre tapon 12x90 V2000 b"
,"Cubre tapon Venado"
,"Estaño"
,"fibra polyester p6/15 (1.6mm/3.3 dtex)"
,"HE-500 A NEGATIVE EXPANDER MADE WITH 100% VANISPERSE"
,"Papel de Empastado 120mm"
,"Papel de Empastado 94 mm"
,"PAPEL EMPASTE DYNADRID 313 ANCHO 109"
,"PASTILLA ANTILLAMA"
,"Plastico Master Amarillo"
,"Plomo Bruto"
,"Polipropileno Virgen Cuyolén 2630"
,"Separador Entek 1.1"
,"Separador Entek 1.3"
,"Separador Entek 1.4"
,"suplemento decantador L1 3mm"
,"suplemento decantador L1 8mm"
,"Tapa 100 IN Generica BCI 31 G-31 Caterpiller"
,"Tapa 110 Baja D N Soyuz"
,"Tapa 110 IN Venado"
,"Tapa 120 DN Venado"
,"Tapa 160 DN Venado Libre Mantenimiento"
,"Tapa 160 IN Venado Libre mantenimiento"
,"Tapa 200 DN Venado"
,"Tapa 50 DN Soyuz (L1)"
,"Tapa 65 DN Soyuz (L2)"
,"Tapa 75 DN Soyuz: L3B Negra"
,"Tapa 80 DN (12-75 Venado)"
,"Tapa 80 IN (12x75 Venado)"
,"Tapa 90 D B Voss2000"
,"Tapa 90 I B Voss2000 H7 Natural +I"
,"Tapita Ciega Kamina Negra"
,"Tapita Kamina Corta Negra Con Pastilla"
,"Tapon Chico Azul c/pastilla Venado"
,"TAPON GRANDE (AZUL) C/PASTILLA"
,"Tapon Kamina M 18 Negro con o ring"
,"Tapon L1 6 en 1 Negro Sellado"
,"Tapon L2 6 en 1 Negro Sellado"
,"Tapon L3 6 en 1 Negro Sellado"
,"Tapon rosca individual V2000"};


	
	public SetCustomMAttribute() {}

	@Override
	protected void prepare() {
		//Products values
			
		//Find products
		for (String productValue : producValues){
			log.log(Level.WARNING, productValue);
			List<MProduct> aux = new Query(getCtx(),MProduct.Table_Name,"name = '"+productValue+"'", get_TrxName()).list();
			if (aux.size() > 1) {
				for (MProduct prod : aux)
				log.log(Level.WARNING, prod+" REPETIDO");
			}
			if (aux.size() == 0){
				prodNotFoundMsg += productValue+" not found \n";
				log.log(Level.WARNING, productValue+" not found");

			}
			else
				products.addAll(aux);
		}
	}

	@Override
	protected String doIt() throws Exception {
		
		//Find custom attribute
		MAttribute customAttribute = new Query(getCtx(),MAttribute.Table_Name, "name= '"+customAttributeName+"'", get_TrxName()).first();
		if (customAttribute == null || customAttribute.getID() == 0)
			return "Custom attribute not found ("+customAttributeName+")";
		
		customAttribute_ID = customAttribute.getID();
		
		
		for (MProduct aProduct : products) {
			MAttributeSet attributeSet = new Query(getCtx(),MAttributeSet.Table_Name, "m_attributeset_id="+aProduct.getM_AttributeSet_ID(), get_TrxName()).first();
			if (attributeSet.getID() == 0){
				log.log(Level.WARNING, aProduct.getValue()+" Attribute Set not found");
				prodAttrNotFoundMsg += aProduct.getValue()+" Attribute Set not found \n";
			}
			else {
				boolean alredyExists = false;
				for (MAttribute attribute : attributeSet.getMAttributes(true,false,true)){
					if (attribute.getM_Attribute_ID() == customAttribute_ID) 
						alredyExists = true;
				}
				
				if (!alredyExists) {
					MAttributeUse use = new MAttributeUse(getCtx(),0,get_TrxName());
					use.setM_Attribute_ID(customAttribute_ID);
					use.setM_AttributeSet_ID(attributeSet.getID());
					use.setIsDescription(true);
					use.setSeqNo(0);
					
					if (!use.save()){
						log.log(Level.SEVERE, "Fallo modificacion de conjunto de atributos para articulo "+aProduct);
					}
				}
			}
			
			
		}
		
		return "Finalizado! \n Productos no encontrados: "+prodNotFoundMsg+". Conjuntos no encontrados: "+prodAttrNotFoundMsg;
	}

}

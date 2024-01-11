using MDBOCBusinessLogic.Maestros;
using MDDBCDataAccess.Maestros;
using MDDTOEntities.Contabilidad;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleOracle
{
    internal class Program
    {
        static string sConexion = ConfigurationManager.ConnectionStrings["cnxOracle"].ConnectionString;
        static DBCGeneric oDbGeneric = new DBCGeneric();
        static  string sEsquema = "";
        static void Main(string[] args)
        {

            fn_TransferirShadow_pods();

            ///fn_CargarEECC();
            //fn_TransferirEECC();
            //fn_CargarEECC();
            //fn_CargarEECC_ORACLE();
            //fn_TransferirEECC_ORACLE();

            //fn_EnviarCorreo("CORREOENVIADO", 1, "REG", "AND CODIGOESTADOORIGEN IN('REG') ");
            // fn_EnviarCorreoCupon("ACTUALIZARCORREOCUPON", 9, "'REG','ANU'");

            //fn_EnvioCorreoGeneral();

            //REGISTRO DE OP
            //fn_EnviarCorreo("CORREOENVIADO", 1, "REG", "AND CODIGOESTADOORIGEN IN('REG') ");

            //LIBERACION DE OP
            //fn_EnviarCorreo("CORREOENVIADOAPROBACION1", 3, "APT", "AND CODIGOESTADOORIGEN IN('REG')  AND NUMEROORDENPEDIDOORACLE IN(8053,8054,8055,8056,8057,8058) ");

            //SOLICITUD DE EXCEPCION
            //fn_EnviarCorreo("ACTUALIZARCORREOEXCEPCION", 2, "PENDA", "AND CODIGOESTADOORIGEN IN('REG') ");

            //fn_EnvioCorreoGeneral();

            /*
            string sPeriodo = "202306";
            string sMes = "";
            string sAnnio = sPeriodo.Substring(0, 4);
            sMes = sPeriodo.Substring(4, 2);

            string sRuta = @"C:\Users\RPA_Entel-PE11\Entel Peru S.A\EntelDrive Gestión de Información - Archivos\\";
            sRuta = sRuta + sAnnio + @"\\";
            DirectoryInfo listDirectory = new DirectoryInfo(sRuta);
            DirectoryInfo[] files = listDirectory.GetDirectories("*");

            
            foreach (DirectoryInfo file in files)
            {
                string[] ArrMes = file.Name.Split('-');
                string sMesFile = ArrMes[0].Trim();

                if (sMesFile == sMes)
                {
                    sRuta = sRuta + sMes +" " +fn_MesDES(sMes) + "\\\\DEACS";
                    DirectoryInfo listDirectory2 = new DirectoryInfo(sRuta);
                    FileInfo[] filesr = listDirectory2.GetFiles("*");

                    foreach (var resfil in filesr)
                    { 
                        //07 - Julio\DEACS
                        string sEx = Path.GetExtension(resfil.FullName);


                    }

                }
            }
            */


            //fn_ProcesarDEACS();

        }
        static string fn_MesDES(string sMes)
        {
            string sMesRES = "";
            
            if (sMes == "01")
            {
                sMesRES = "-ENERO";
            }
            else if (sMes == "02")
            {
                sMesRES = "-FEBRERO";
            }
            else if (sMes == "03")
            {
                sMesRES = "-MARXO";
            }
            else if (sMes == "04")
            {
                sMesRES = "-ABRIL";
            }
            else if (sMes == "05")
            {
                sMesRES = "-MAYO";
            }
            else if (sMes == "06")
            {
                sMesRES = "- JUNIO";
            }
            else if (sMes == "07")
            {
                sMesRES = "-Julio";
            }
            else if (sMes == "08")
            {
                sMesRES = "-AGOSTO";
            }
            else if (sMes == "09")
            {
                sMesRES = "-SEPTIEMBRE";
            }
            else if (sMes == "10")
            {
                sMesRES = "-OCTUBRE";
            }
            else if (sMes == "11")
            {
                sMesRES = "-NOVIEMBRE";
            }
            else if (sMes == "12")
            {
                sMesRES = "-DICIEMBRE";
            }


            return sMesRES;
        }

        /*
         De servidor "Shadow": 
        Tabla: PLC_INF.HOM_INAR
        Hacia servidor: "Pods_Lm"
        Tabla: CI_INAR_HIST
         */

        static void fn_TransferirShadow_pods()
        {

            try
            {

            
            string sPeriodo = ConfigurationManager.AppSettings["CONF_PERIODO"].ToString();
            DataTable oMP_DEACS_ACUM = fn_ObtenerResultado("  SELECT VCHC_CONTRATO FROM SHADOW_HOM_INAR WHERE NUMPERIODO  = '" + sPeriodo + "' ");

            
            string sContrato = ""; 
            int xTotal = oMP_DEACS_ACUM.Rows.Count;
            int xTotal2 = 1;
            foreach (DataRow oRows in oMP_DEACS_ACUM.Rows)
            {
                if (xTotal == xTotal2)
                    sContrato = oRows["VCHC_CONTRATO"].ToString();
                else
                    sContrato = oRows["VCHC_CONTRATO"].ToString() + ",";
                xTotal2++;
            }


            string sScript = "";

            if(sContrato.Length > 0)
            {
                sScript= "  SELECT NUMPERIODO,FECFECHAPROCESO,FECFECHAACTIVACION,VCHRAZONSOCIAL,VCHC_CONTRATO,VCHIMEI,VCHIMEI_BSCS,VCHTELEFONO,VCHMODELOEQUIPO,VCHC_PLAN,VCHN_PLAN,VCHESTADOINAR,VCHMOVIMIENTOS,NUMGROSS,VCHVENDEDOR,VCHTIPODOCUMENTO,VCHDOCUMENTO,NUMNRO_ORDEN,VCHPRODUCT,NUMRENTABASICA,NUMRENTAIGV,VCHSEGMENTO,VCHMODOPAGO,VCHTECNOLOGIA,VCHCLASIFICACIONRENTA,VCHPLAN_BLINDAJE,VCHVENDEDOR_PACKSIM,VCHPROMO_CHIPS,NUMCARGOFIJO,VCHTIPOVENTA,VCHCOMBO_CANAL,VCHPROD_VENTAREGULAR,VCHDWH_CODIGOORDEN,VCHDWH_ORDENCREADOPOR,VCHDWH_NOMBRECONSULTOR,VCHDWH_PRODUCTO,VCHMODEL_F,VCHSIM_DESBLOQUEADO,VCHPTVJAVA_PRODUCTO,VCHPTVJAVA_SKU,VCHPTVJAVA_PROMOTOR,VCHPORTA_MODOPAGOORIGEN,VCHPORTA_CEDENTE,VCHPORTA_RECEPTOR,VCHJER_PDV,VCHJER_GERENCIACANAL,VCHJER_CANALVENTA,VCHJER_KAM,VCHJER_TERRITORIO,VCHJER_DIVTERRITORIO,VCHJER_CADENADEALER,VCHJER_SOCIODENEGOCIO,VCHJER_TIPOTIENDA,VCHJER_DEPARTAMENTO,VCHJER_PROVINCIA,VCHJER_CIUDAD,VCHJER_DISTRITO,VCHJER_JEFENEGOCIO,VCHTERMINAL_GAMA,VCHTERMINAL_MARCA,VCHTERMINAL_MODELO,VCHTERMINAL_TIPOEQUIPO,VCHTERMINAL_NOMBREEQUIPO,VCHTERMINAL_TECNOLOGIA,VCHVEN_PACKSIM_DEP,VCHVEN_PACKSIM_PROV,VCHVEN_PACKSIM_DIST,VCHTIPOCONTRIBUYENTE,NUMDWH_PRECIOLISTA,NUMDWH_PRECIOPAGADO,NUMDWH_SUBTOTAL,VCHCORP_CANALVENTA,VCHCORP_VISTACLIENTE,VCHCORP_VISTANEGOCIO,NUMCANTIDADRECARGAS,NUMMONTORECARGAS,NUMTOTAL_MES_ANT,NUMTOTAL_MES_ACT,NUMTOTAL_FINAL,NUMDESCUENTO_2DALINEA,VCHTIPO_DESC_2DALINEA,VCHJER_NIVEL_TC,VCHCELDA_DEP,VCHCELDA_PROV,VCHCELDA_DIST,VCHCELDA_TIPOACTIVACION,VCHCELDA_GNT,VCHCELDA_USER_ID,VCHCELDA_TIPO_VENTA,VCHCELDA_SSNN,VCHCELDA_KAM,VCHCELDA_TOP_PDV,VCHSUSCRIPCIONESPLAN,VCHPREPAGO_TIPOCHIP,VCHPREPAGO_REGION_DEPART,NUMPRIMERA_REC,VCHTIPO_NEGOCIO,NUMSEMANA_ANHIO,VCHSINCRITERIOBASE,VCHJER_CLUSTER_GLOBAL,VCHJER_CLUSTER,NUMREC_QREC7,NUMREC_MONTO7,NUMREC_QREC15,NUMREC_MONTO15,VCHRECARGA_7,VCHRECARGA_15,VCHEMPRESAS_JER_DEPART,VCHEMPRESAS_JER_PROVIN,VCHEMPRESAS_JER_DISTRI,VCHCORP_DEPARTAMENTO,VCHCORP_PROVINCIA,VCHCORP_DISTRITO,FECFECH_CREACION_PORTA,NUMVEP_CUOTA,NUMVEP_PAGOTOTAL,NUMVEP_CUOTA_INICIAL,VCHVEP_FLAG_NUEVO,VCHVEP_FLAG_TOTAL,VCHCELDA_IMSI_SELLER,VCHCELDA_IMEI_SELLER,VCHSISTEMA_FUENTE,NUMPORTIN,VCHBONO,VCHCHNL_TDE,VCHACTIVATION_TYPE,VCHUSERNAME,VCHFLAG_APAGON,FECFECHA_APAGON,VCHEST_APAGON,VCHFLAG_REC_FECACT,VCHDESC_ORDER,VCHTIPORUC,VCHDOCUMENTO_CIERRE,VCHVENDEDOR_PACKSIM_OR,VCHENTEL_PRO,VCHPLAN_CHIP,VCHCICLOFACTURACION,VCHCODIGOCOMPANIA,VCHC_CONTRATOFS,VCHCODIGO,VCHNUMERO_RUC,VCHFLAG_ALTO_VALOR,VCHALMACEN,VCHPDV_PICKUP,VCHDIVTR_PICKUP,NUMFLAG_APERTURA,VCHFLAG_PRODUCTO,VCHNIVEL_GRUPO,VCHCLUSTER,FECFECHACREACION,VCHLLAA_BASE_CAPTURA,VCHVENDEDORDNI,VCHJER_CANALVENTATLV,NUMFLAGT0,VCHTYPEOFSALE,NUMFLAGRFT,VCHDETALLECAMPANA,NUMFLAG_OFERTA,VCHCLUSTER2,VCHCANAL2,VCHIMSISELLER,VCHAA_WEB,VCHJER_PDVRETAIL,VCHFLAG_LLA_RENO,VCHJER_CAMPANACANAL,VCHJER_CANALCHILE,VCHJER_CANALVENTA2,NUMMONTO_ORDEN,VCHFLAG_RECARGA7DIAS,VCHFLAG_LM,VCHJER_CAMPANAAGRUPADA,VCHFLAG_VALIDACIONBIO,VCHFLAG_UPSELLING,VCHFLAG_VEP2,NUMRENTAIGV_NETO,NUMRENTAIGV_NESTRUCTURAL,VCHDNI_LIDER,VCHN_PLANPILOTO,NUMRENTAIGV_NETOT,VCHDEP_ENTREGA_OL,VCHPROV_ENTREGA_OL,VCHDIST_ENTREGA_OL,VCHCLUSTER_DELIVERY,VCHFLAG_RENODIARIO,VCHFLAG_SINTERES,VCHCONCEPTO_DESC,NUMVALOR_DESC FROM PLC_INF.HOM_INAR WHERE NUMPERIODO  = '" + sPeriodo + "' AND  VCHC_CONTRATO  NOT IN ('" + sContrato + "'); ";
            }
            else
            {
                sScript = "  SELECT NUMPERIODO,FECFECHAPROCESO,FECFECHAACTIVACION,VCHRAZONSOCIAL,VCHC_CONTRATO,VCHIMEI,VCHIMEI_BSCS,VCHTELEFONO,VCHMODELOEQUIPO,VCHC_PLAN,VCHN_PLAN,VCHESTADOINAR,VCHMOVIMIENTOS,NUMGROSS,VCHVENDEDOR,VCHTIPODOCUMENTO,VCHDOCUMENTO,NUMNRO_ORDEN,VCHPRODUCT,NUMRENTABASICA,NUMRENTAIGV,VCHSEGMENTO,VCHMODOPAGO,VCHTECNOLOGIA,VCHCLASIFICACIONRENTA,VCHPLAN_BLINDAJE,VCHVENDEDOR_PACKSIM,VCHPROMO_CHIPS,NUMCARGOFIJO,VCHTIPOVENTA,VCHCOMBO_CANAL,VCHPROD_VENTAREGULAR,VCHDWH_CODIGOORDEN,VCHDWH_ORDENCREADOPOR,VCHDWH_NOMBRECONSULTOR,VCHDWH_PRODUCTO,VCHMODEL_F,VCHSIM_DESBLOQUEADO,VCHPTVJAVA_PRODUCTO,VCHPTVJAVA_SKU,VCHPTVJAVA_PROMOTOR,VCHPORTA_MODOPAGOORIGEN,VCHPORTA_CEDENTE,VCHPORTA_RECEPTOR,VCHJER_PDV,VCHJER_GERENCIACANAL,VCHJER_CANALVENTA,VCHJER_KAM,VCHJER_TERRITORIO,VCHJER_DIVTERRITORIO,VCHJER_CADENADEALER,VCHJER_SOCIODENEGOCIO,VCHJER_TIPOTIENDA,VCHJER_DEPARTAMENTO,VCHJER_PROVINCIA,VCHJER_CIUDAD,VCHJER_DISTRITO,VCHJER_JEFENEGOCIO,VCHTERMINAL_GAMA,VCHTERMINAL_MARCA,VCHTERMINAL_MODELO,VCHTERMINAL_TIPOEQUIPO,VCHTERMINAL_NOMBREEQUIPO,VCHTERMINAL_TECNOLOGIA,VCHVEN_PACKSIM_DEP,VCHVEN_PACKSIM_PROV,VCHVEN_PACKSIM_DIST,VCHTIPOCONTRIBUYENTE,NUMDWH_PRECIOLISTA,NUMDWH_PRECIOPAGADO,NUMDWH_SUBTOTAL,VCHCORP_CANALVENTA,VCHCORP_VISTACLIENTE,VCHCORP_VISTANEGOCIO,NUMCANTIDADRECARGAS,NUMMONTORECARGAS,NUMTOTAL_MES_ANT,NUMTOTAL_MES_ACT,NUMTOTAL_FINAL,NUMDESCUENTO_2DALINEA,VCHTIPO_DESC_2DALINEA,VCHJER_NIVEL_TC,VCHCELDA_DEP,VCHCELDA_PROV,VCHCELDA_DIST,VCHCELDA_TIPOACTIVACION,VCHCELDA_GNT,VCHCELDA_USER_ID,VCHCELDA_TIPO_VENTA,VCHCELDA_SSNN,VCHCELDA_KAM,VCHCELDA_TOP_PDV,VCHSUSCRIPCIONESPLAN,VCHPREPAGO_TIPOCHIP,VCHPREPAGO_REGION_DEPART,NUMPRIMERA_REC,VCHTIPO_NEGOCIO,NUMSEMANA_ANHIO,VCHSINCRITERIOBASE,VCHJER_CLUSTER_GLOBAL,VCHJER_CLUSTER,NUMREC_QREC7,NUMREC_MONTO7,NUMREC_QREC15,NUMREC_MONTO15,VCHRECARGA_7,VCHRECARGA_15,VCHEMPRESAS_JER_DEPART,VCHEMPRESAS_JER_PROVIN,VCHEMPRESAS_JER_DISTRI,VCHCORP_DEPARTAMENTO,VCHCORP_PROVINCIA,VCHCORP_DISTRITO,FECFECH_CREACION_PORTA,NUMVEP_CUOTA,NUMVEP_PAGOTOTAL,NUMVEP_CUOTA_INICIAL,VCHVEP_FLAG_NUEVO,VCHVEP_FLAG_TOTAL,VCHCELDA_IMSI_SELLER,VCHCELDA_IMEI_SELLER,VCHSISTEMA_FUENTE,NUMPORTIN,VCHBONO,VCHCHNL_TDE,VCHACTIVATION_TYPE,VCHUSERNAME,VCHFLAG_APAGON,FECFECHA_APAGON,VCHEST_APAGON,VCHFLAG_REC_FECACT,VCHDESC_ORDER,VCHTIPORUC,VCHDOCUMENTO_CIERRE,VCHVENDEDOR_PACKSIM_OR,VCHENTEL_PRO,VCHPLAN_CHIP,VCHCICLOFACTURACION,VCHCODIGOCOMPANIA,VCHC_CONTRATOFS,VCHCODIGO,VCHNUMERO_RUC,VCHFLAG_ALTO_VALOR,VCHALMACEN,VCHPDV_PICKUP,VCHDIVTR_PICKUP,NUMFLAG_APERTURA,VCHFLAG_PRODUCTO,VCHNIVEL_GRUPO,VCHCLUSTER,FECFECHACREACION,VCHLLAA_BASE_CAPTURA,VCHVENDEDORDNI,VCHJER_CANALVENTATLV,NUMFLAGT0,VCHTYPEOFSALE,NUMFLAGRFT,VCHDETALLECAMPANA,NUMFLAG_OFERTA,VCHCLUSTER2,VCHCANAL2,VCHIMSISELLER,VCHAA_WEB,VCHJER_PDVRETAIL,VCHFLAG_LLA_RENO,VCHJER_CAMPANACANAL,VCHJER_CANALCHILE,VCHJER_CANALVENTA2,NUMMONTO_ORDEN,VCHFLAG_RECARGA7DIAS,VCHFLAG_LM,VCHJER_CAMPANAAGRUPADA,VCHFLAG_VALIDACIONBIO,VCHFLAG_UPSELLING,VCHFLAG_VEP2,NUMRENTAIGV_NETO,NUMRENTAIGV_NESTRUCTURAL,VCHDNI_LIDER,VCHN_PLANPILOTO,NUMRENTAIGV_NETOT,VCHDEP_ENTREGA_OL,VCHPROV_ENTREGA_OL,VCHDIST_ENTREGA_OL,VCHCLUSTER_DELIVERY,VCHFLAG_RENODIARIO,VCHFLAG_SINTERES,VCHCONCEPTO_DESC,NUMVALOR_DESC FROM PLC_INF.HOM_INAR WHERE NUMPERIODO  = '" + sPeriodo + "' ";
            }
                 Console.WriteLine("CONECTANDOSE A SHADOW ..." );

                sScript = "  SELECT count(1) FROM PLC_INF.HOM_INAR WHERE NUMPERIODO  = '" + sPeriodo + "' ";

                Console.WriteLine(sScript);
                DataTable oMP_DEACS_ACUM_SHADOW = fn_ObtenerResultadoShadow(sScript);

            Console.WriteLine("TOTAL DE REGISTROS EN SHADOWS ..." + oMP_DEACS_ACUM_SHADOW.Rows.Count);


                Console.WriteLine(sScript);
                Thread.Sleep(19000);


                int cInsercion = 1;

            foreach(DataRow oRows_insert in oMP_DEACS_ACUM_SHADOW.Rows) {

                string sScript_Insert = "  INSERT INTO SHADOW_HOM_INAR (NUMPERIODO,FECFECHAPROCESO,FECFECHAACTIVACION,VCHRAZONSOCIAL,VCHC_CONTRATO,VCHIMEI,VCHIMEI_BSCS,VCHTELEFONO,VCHMODELOEQUIPO,VCHC_PLAN,VCHN_PLAN,VCHESTADOINAR,VCHMOVIMIENTOS,NUMGROSS,VCHVENDEDOR,VCHTIPODOCUMENTO,VCHDOCUMENTO,NUMNRO_ORDEN,VCHPRODUCT,NUMRENTABASICA,NUMRENTAIGV,VCHSEGMENTO,VCHMODOPAGO,VCHTECNOLOGIA,VCHCLASIFICACIONRENTA,VCHPLAN_BLINDAJE,VCHVENDEDOR_PACKSIM,VCHPROMO_CHIPS,NUMCARGOFIJO,VCHTIPOVENTA,VCHCOMBO_CANAL,VCHPROD_VENTAREGULAR,VCHDWH_CODIGOORDEN,VCHDWH_ORDENCREADOPOR,VCHDWH_NOMBRECONSULTOR,VCHDWH_PRODUCTO,VCHMODEL_F,VCHSIM_DESBLOQUEADO,VCHPTVJAVA_PRODUCTO,VCHPTVJAVA_SKU,VCHPTVJAVA_PROMOTOR,VCHPORTA_MODOPAGOORIGEN,VCHPORTA_CEDENTE,VCHPORTA_RECEPTOR,VCHJER_PDV,VCHJER_GERENCIACANAL,VCHJER_CANALVENTA,VCHJER_KAM,VCHJER_TERRITORIO,VCHJER_DIVTERRITORIO,VCHJER_CADENADEALER,VCHJER_SOCIODENEGOCIO,VCHJER_TIPOTIENDA,VCHJER_DEPARTAMENTO,VCHJER_PROVINCIA,VCHJER_CIUDAD,VCHJER_DISTRITO,VCHJER_JEFENEGOCIO,VCHTERMINAL_GAMA,VCHTERMINAL_MARCA,VCHTERMINAL_MODELO,VCHTERMINAL_TIPOEQUIPO,VCHTERMINAL_NOMBREEQUIPO,VCHTERMINAL_TECNOLOGIA,VCHVEN_PACKSIM_DEP,VCHVEN_PACKSIM_PROV,VCHVEN_PACKSIM_DIST,VCHTIPOCONTRIBUYENTE,NUMDWH_PRECIOLISTA,NUMDWH_PRECIOPAGADO,NUMDWH_SUBTOTAL,VCHCORP_CANALVENTA,VCHCORP_VISTACLIENTE,VCHCORP_VISTANEGOCIO,NUMCANTIDADRECARGAS,NUMMONTORECARGAS,NUMTOTAL_MES_ANT,NUMTOTAL_MES_ACT,NUMTOTAL_FINAL,NUMDESCUENTO_2DALINEA,VCHTIPO_DESC_2DALINEA,VCHJER_NIVEL_TC,VCHCELDA_DEP,VCHCELDA_PROV,VCHCELDA_DIST,VCHCELDA_TIPOACTIVACION,VCHCELDA_GNT,VCHCELDA_USER_ID,VCHCELDA_TIPO_VENTA,VCHCELDA_SSNN,VCHCELDA_KAM,VCHCELDA_TOP_PDV,VCHSUSCRIPCIONESPLAN,VCHPREPAGO_TIPOCHIP,VCHPREPAGO_REGION_DEPART,NUMPRIMERA_REC,VCHTIPO_NEGOCIO,NUMSEMANA_ANHIO,VCHSINCRITERIOBASE,VCHJER_CLUSTER_GLOBAL,VCHJER_CLUSTER,NUMREC_QREC7,NUMREC_MONTO7,NUMREC_QREC15,NUMREC_MONTO15,VCHRECARGA_7,VCHRECARGA_15,VCHEMPRESAS_JER_DEPART,VCHEMPRESAS_JER_PROVIN,VCHEMPRESAS_JER_DISTRI,VCHCORP_DEPARTAMENTO,VCHCORP_PROVINCIA,VCHCORP_DISTRITO,FECFECH_CREACION_PORTA,NUMVEP_CUOTA,NUMVEP_PAGOTOTAL,NUMVEP_CUOTA_INICIAL,VCHVEP_FLAG_NUEVO,VCHVEP_FLAG_TOTAL,VCHCELDA_IMSI_SELLER,VCHCELDA_IMEI_SELLER,VCHSISTEMA_FUENTE,NUMPORTIN,VCHBONO,VCHCHNL_TDE,VCHACTIVATION_TYPE,VCHUSERNAME,VCHFLAG_APAGON,FECFECHA_APAGON,VCHEST_APAGON,VCHFLAG_REC_FECACT,VCHDESC_ORDER,VCHTIPORUC,VCHDOCUMENTO_CIERRE,VCHVENDEDOR_PACKSIM_OR,VCHENTEL_PRO,VCHPLAN_CHIP,VCHCICLOFACTURACION,VCHCODIGOCOMPANIA,VCHC_CONTRATOFS,VCHCODIGO,VCHNUMERO_RUC,VCHFLAG_ALTO_VALOR,VCHALMACEN,VCHPDV_PICKUP,VCHDIVTR_PICKUP,NUMFLAG_APERTURA,VCHFLAG_PRODUCTO,VCHNIVEL_GRUPO,VCHCLUSTER,FECFECHACREACION,VCHLLAA_BASE_CAPTURA,VCHVENDEDORDNI,VCHJER_CANALVENTATLV,NUMFLAGT0,VCHTYPEOFSALE,NUMFLAGRFT,VCHDETALLECAMPANA,NUMFLAG_OFERTA,VCHCLUSTER2,VCHCANAL2,VCHIMSISELLER,VCHAA_WEB,VCHJER_PDVRETAIL,VCHFLAG_LLA_RENO,VCHJER_CAMPANACANAL,VCHJER_CANALCHILE,VCHJER_CANALVENTA2,NUMMONTO_ORDEN,VCHFLAG_RECARGA7DIAS,VCHFLAG_LM,VCHJER_CAMPANAAGRUPADA,VCHFLAG_VALIDACIONBIO,VCHFLAG_UPSELLING,VCHFLAG_VEP2,NUMRENTAIGV_NETO,NUMRENTAIGV_NESTRUCTURAL,VCHDNI_LIDER,VCHN_PLANPILOTO,NUMRENTAIGV_NETOT,VCHDEP_ENTREGA_OL,VCHPROV_ENTREGA_OL,VCHDIST_ENTREGA_OL,VCHCLUSTER_DELIVERY,VCHFLAG_RENODIARIO,VCHFLAG_SINTERES,VCHCONCEPTO_DESC,NUMVALOR_DESC )  VALUES(" +
                        oRows_insert["NUMPERIODO"] + "," + oRows_insert["FECFECHAPROCESO"] + "," + oRows_insert["FECFECHAACTIVACION"] + "," + oRows_insert["VCHRAZONSOCIAL"] + "," + oRows_insert["VCHC_CONTRATO"] + "," + oRows_insert["VCHIMEI"] + "," + oRows_insert["VCHIMEI_BSCS"] + "," + oRows_insert["VCHTELEFONO"] + "," + oRows_insert["VCHMODELOEQUIPO"] + "," + oRows_insert["VCHC_PLAN"] + "," + oRows_insert["VCHN_PLAN"] + "," + oRows_insert["VCHESTADOINAR"] + "," + oRows_insert["VCHMOVIMIENTOS"] + "," + oRows_insert["NUMGROSS"] + "," + oRows_insert["VCHVENDEDOR"] + "," + oRows_insert["VCHTIPODOCUMENTO"] + "," + oRows_insert["VCHDOCUMENTO"] + "," + oRows_insert["NUMNRO_ORDEN"] + "," + oRows_insert["VCHPRODUCT"] + "," + oRows_insert["NUMRENTABASICA"] + "," + oRows_insert["NUMRENTAIGV"] + "," + oRows_insert["VCHSEGMENTO"] + "," + oRows_insert["VCHMODOPAGO"] + "," + oRows_insert["VCHTECNOLOGIA"] + "," + oRows_insert["VCHCLASIFICACIONRENTA"] + "," + oRows_insert["VCHPLAN_BLINDAJE"] + "," + oRows_insert["VCHVENDEDOR_PACKSIM"] + "," + oRows_insert["VCHPROMO_CHIPS"] + "," + oRows_insert["NUMCARGOFIJO"] + "," + oRows_insert["VCHTIPOVENTA"] + "," + oRows_insert["VCHCOMBO_CANAL"] + "," + oRows_insert["VCHPROD_VENTAREGULAR"] + "," + oRows_insert["VCHDWH_CODIGOORDEN"] + "," + oRows_insert["VCHDWH_ORDENCREADOPOR"] + "," + oRows_insert["VCHDWH_NOMBRECONSULTOR"] + "," + oRows_insert["VCHDWH_PRODUCTO"] + "," + oRows_insert["VCHMODEL_F"] + "," + oRows_insert["VCHSIM_DESBLOQUEADO"] + "," + oRows_insert["VCHPTVJAVA_PRODUCTO"] + "," + oRows_insert["VCHPTVJAVA_SKU"] + "," + oRows_insert["VCHPTVJAVA_PROMOTOR"] + "," + oRows_insert["VCHPORTA_MODOPAGOORIGEN"] + "," + oRows_insert["VCHPORTA_CEDENTE"] + "," + oRows_insert["VCHPORTA_RECEPTOR"] + "," + oRows_insert["VCHJER_PDV"] + "," + oRows_insert["VCHJER_GERENCIACANAL"] + "," + oRows_insert["VCHJER_CANALVENTA"] + "," + oRows_insert["VCHJER_KAM"] + "," + oRows_insert["VCHJER_TERRITORIO"] + "," + oRows_insert["VCHJER_DIVTERRITORIO"] + "," + oRows_insert["VCHJER_CADENADEALER"] + "," + oRows_insert["VCHJER_SOCIODENEGOCIO"] + "," + oRows_insert["VCHJER_TIPOTIENDA"] + "," + oRows_insert["VCHJER_DEPARTAMENTO"] + "," + oRows_insert["VCHJER_PROVINCIA"] + "," + oRows_insert["VCHJER_CIUDAD"] + "," + oRows_insert["VCHJER_DISTRITO"] + "," + oRows_insert["VCHJER_JEFENEGOCIO"] + "," + oRows_insert["VCHTERMINAL_GAMA"] + "," + oRows_insert["VCHTERMINAL_MARCA"] + "," + oRows_insert["VCHTERMINAL_MODELO"] + "," + oRows_insert["VCHTERMINAL_TIPOEQUIPO"] + "," + oRows_insert["VCHTERMINAL_NOMBREEQUIPO"] + "," + oRows_insert["VCHTERMINAL_TECNOLOGIA"] + "," + oRows_insert["VCHVEN_PACKSIM_DEP"] + "," + oRows_insert["VCHVEN_PACKSIM_PROV"] + "," + oRows_insert["VCHVEN_PACKSIM_DIST"] + "," + oRows_insert["VCHTIPOCONTRIBUYENTE"] + "," + oRows_insert["NUMDWH_PRECIOLISTA"] + "," + oRows_insert["NUMDWH_PRECIOPAGADO"] + "," + oRows_insert["NUMDWH_SUBTOTAL"] + "," + oRows_insert["VCHCORP_CANALVENTA"] + "," + oRows_insert["VCHCORP_VISTACLIENTE"] + "," + oRows_insert["VCHCORP_VISTANEGOCIO"] + "," + oRows_insert["NUMCANTIDADRECARGAS"] + "," + oRows_insert["NUMMONTORECARGAS"] + "," + oRows_insert["NUMTOTAL_MES_ANT"] + "," + oRows_insert["NUMTOTAL_MES_ACT"] + "," + oRows_insert["NUMTOTAL_FINAL"] + "," + oRows_insert["NUMDESCUENTO_2DALINEA"] + "," + oRows_insert["VCHTIPO_DESC_2DALINEA"] + "," + oRows_insert["VCHJER_NIVEL_TC"] + "," + oRows_insert["VCHCELDA_DEP"] + "," + oRows_insert["VCHCELDA_PROV"] + "," + oRows_insert["VCHCELDA_DIST"] + "," + oRows_insert["VCHCELDA_TIPOACTIVACION"] + "," + oRows_insert["VCHCELDA_GNT"] + "," + oRows_insert["VCHCELDA_USER_ID"] + "," + oRows_insert["VCHCELDA_TIPO_VENTA"] + "," + oRows_insert["VCHCELDA_SSNN"] + "," + oRows_insert["VCHCELDA_KAM"] + "," + oRows_insert["VCHCELDA_TOP_PDV"] + "," + oRows_insert["VCHSUSCRIPCIONESPLAN"] + "," + oRows_insert["VCHPREPAGO_TIPOCHIP"] + "," + oRows_insert["VCHPREPAGO_REGION_DEPART"] + "," + oRows_insert["NUMPRIMERA_REC"] + "," + oRows_insert["VCHTIPO_NEGOCIO"] + "," + oRows_insert["NUMSEMANA_ANHIO"] + "," + oRows_insert["VCHSINCRITERIOBASE"] + "," + oRows_insert["VCHJER_CLUSTER_GLOBAL"] + "," + oRows_insert["VCHJER_CLUSTER"] + "," + oRows_insert["NUMREC_QREC7"] + "," + oRows_insert["NUMREC_MONTO7"] + "," + oRows_insert["NUMREC_QREC15"] + "," + oRows_insert["NUMREC_MONTO15"] + "," + oRows_insert["VCHRECARGA_7"] + "," + oRows_insert["VCHRECARGA_15"] + "," + oRows_insert["VCHEMPRESAS_JER_DEPART"] + "," + oRows_insert["VCHEMPRESAS_JER_PROVIN"] + "," + oRows_insert["VCHEMPRESAS_JER_DISTRI"] + "," + oRows_insert["VCHCORP_DEPARTAMENTO"] + "," + oRows_insert["VCHCORP_PROVINCIA"] + "," + oRows_insert["VCHCORP_DISTRITO"] + "," + oRows_insert["FECFECH_CREACION_PORTA"] + "," + oRows_insert["NUMVEP_CUOTA"] + "," + oRows_insert["NUMVEP_PAGOTOTAL"] + "," + oRows_insert["NUMVEP_CUOTA_INICIAL"] + "," + oRows_insert["VCHVEP_FLAG_NUEVO"] + "," + oRows_insert["VCHVEP_FLAG_TOTAL"] + "," + oRows_insert["VCHCELDA_IMSI_SELLER"] + "," + oRows_insert["VCHCELDA_IMEI_SELLER"] + "," + oRows_insert["VCHSISTEMA_FUENTE"] + "," + oRows_insert["NUMPORTIN"] + "," + oRows_insert["VCHBONO"] + "," + oRows_insert["VCHCHNL_TDE"] + "," + oRows_insert["VCHACTIVATION_TYPE"] + "," + oRows_insert["VCHUSERNAME"] + "," + oRows_insert["VCHFLAG_APAGON"] + "," + oRows_insert["FECFECHA_APAGON"] + "," + oRows_insert["VCHEST_APAGON"] + "," + oRows_insert["VCHFLAG_REC_FECACT"] + "," + oRows_insert["VCHDESC_ORDER"] + "," + oRows_insert["VCHTIPORUC"] + "," + oRows_insert["VCHDOCUMENTO_CIERRE"] + "," + oRows_insert["VCHVENDEDOR_PACKSIM_OR"] + "," + oRows_insert["VCHENTEL_PRO"] + "," + oRows_insert["VCHPLAN_CHIP"] + "," + oRows_insert["VCHCICLOFACTURACION"] + "," + oRows_insert["VCHCODIGOCOMPANIA"] + "," + oRows_insert["VCHC_CONTRATOFS"] + "," + oRows_insert["VCHCODIGO"] + "," + oRows_insert["VCHNUMERO_RUC"] + "," + oRows_insert["VCHFLAG_ALTO_VALOR"] + "," + oRows_insert["VCHALMACEN"] + "," + oRows_insert["VCHPDV_PICKUP"] + "," + oRows_insert["VCHDIVTR_PICKUP"] + "," + oRows_insert["NUMFLAG_APERTURA"] + "," + oRows_insert["VCHFLAG_PRODUCTO"] + "," + oRows_insert["VCHNIVEL_GRUPO"] + "," + oRows_insert["VCHCLUSTER"] + "," + oRows_insert["FECFECHACREACION"] + "," + oRows_insert["VCHLLAA_BASE_CAPTURA"] + "," + oRows_insert["VCHVENDEDORDNI"] + "," + oRows_insert["VCHJER_CANALVENTATLV"] + "," + oRows_insert["NUMFLAGT0"] + "," + oRows_insert["VCHTYPEOFSALE"] + "," + oRows_insert["NUMFLAGRFT"] + "," + oRows_insert["VCHDETALLECAMPANA"] + "," + oRows_insert["NUMFLAG_OFERTA"] + "," + oRows_insert["VCHCLUSTER2"] + "," + oRows_insert["VCHCANAL2"] + "," + oRows_insert["VCHIMSISELLER"] + "," + oRows_insert["VCHAA_WEB"] + "," + oRows_insert["VCHJER_PDVRETAIL"] + "," + oRows_insert["VCHFLAG_LLA_RENO"] + "," + oRows_insert["VCHJER_CAMPANACANAL"] + "," + oRows_insert["VCHJER_CANALCHILE"] + "," + oRows_insert["VCHJER_CANALVENTA2"] + "," + oRows_insert["NUMMONTO_ORDEN"] + "," + oRows_insert["VCHFLAG_RECARGA7DIAS"] + "," + oRows_insert["VCHFLAG_LM"] + "," + oRows_insert["VCHJER_CAMPANAAGRUPADA"] + "," + oRows_insert["VCHFLAG_VALIDACIONBIO"] + "," + oRows_insert["VCHFLAG_UPSELLING"] + "," + oRows_insert["VCHFLAG_VEP2"] + "," + oRows_insert["NUMRENTAIGV_NETO"] + "," + oRows_insert["NUMRENTAIGV_NESTRUCTURAL"] + "," + oRows_insert["VCHDNI_LIDER"] + "," + oRows_insert["VCHN_PLANPILOTO"] + "," + oRows_insert["NUMRENTAIGV_NETOT"] + "," + oRows_insert["VCHDEP_ENTREGA_OL"] + "," + oRows_insert["VCHPROV_ENTREGA_OL"] + "," + oRows_insert["VCHDIST_ENTREGA_OL"] + "," + oRows_insert["VCHCLUSTER_DELIVERY"] + "," + oRows_insert["VCHFLAG_RENODIARIO"] + "," + oRows_insert["VCHFLAG_SINTERES"] + "," + oRows_insert["VCHCONCEPTO_DESC"] + "," +
                        oRows_insert["NUMVALOR_DESC"];

                fn_ObtenerResultado(sScript_Insert);

                Console.WriteLine("insertados " + cInsercion + " de "+ oMP_DEACS_ACUM_SHADOW.Rows.Count);

                cInsercion++;
            }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.ReadLine();
            }

        }


        static void fn_ProcesarDEACS()
        {

            DataTable oMP_DEACS_ACUM = fn_ObtenerResultado("  SELECT C_CONTRATO FROM ODS_CDG.MP_DEACS_ACUM WHERE PERIODO  = '202306' AND (FECHAPROCESO-FECHAACTIVACION) <= 180 AND ESTADOINAR = 'DEAC' ");

            string sContrato = "";
            int xTotal=oMP_DEACS_ACUM.Rows.Count;
            int xTotal2 = 1; 
            foreach (DataRow oRows in oMP_DEACS_ACUM.Rows) {
                if(xTotal== xTotal2)
                sContrato = oRows["C_CONTRATO"].ToString();
                else
                    sContrato = oRows["C_CONTRATO"].ToString()+",";
                xTotal2++;
            }

            DataTable HOM_INAR = fn_ObtenerResultadoShadow("  SELECT C_CONTRATO FROM PLC_INF.HOM_INAR WHERE " +
                "VCHC_CONTRATO  IN ("+ sContrato + " )");
            


        }



        public static DataTable JoinDataTable(DataTable dataTable1, DataTable dataTable2, string joinField)
        {
            var dt = new DataTable();
            var joinTable = from t1 in dataTable1.AsEnumerable()
                            join t2 in dataTable2.AsEnumerable()
                                on t1[joinField] equals t2[joinField]
                            select new { t1, t2 };

            foreach (DataColumn col in dataTable1.Columns)
                dt.Columns.Add(col.ColumnName, typeof(string));

            dt.Columns.Remove(joinField);

            foreach (DataColumn col in dataTable2.Columns)
                dt.Columns.Add(col.ColumnName, typeof(string));

            foreach (var row in joinTable)
            {
                var newRow = dt.NewRow();
                newRow.ItemArray = row.t1.ItemArray.Union(row.t2.ItemArray).ToArray();
                dt.Rows.Add(newRow);
            }
            return dt;
        }

        static void fn_EnvioCorreoGeneral()
        {

            //REGISTRO DE OP
            fn_EnviarCorreo("CORREOENVIADO", 1, "REG", "AND CODIGOESTADOORIGEN IN('REG') ");

            //SOLICITUD DE EXCEPCION
            fn_EnviarCorreo("ACTUALIZARCORREOEXCEPCION", 2, "PENDA", "AND CODIGOESTADOORIGEN IN('REG') ");

            //LIBERACION DE OP
            fn_EnviarCorreo("CORREOENVIADOAPROBACION1", 3, "APT", "AND CODIGOESTADOORIGEN IN('REG') ");

            //APROBACION DE EXCEPCION
            fn_EnviarCorreo("ACTUALIZARCORREOEXCEPCION2", 7, "REG", "AND CODIGOESTADOORIGEN IN('PENDA') ");

            //ENVIO DE ESTADO DE CUPON
            fn_EnviarCorreoCupon("ACTUALIZARCORREOCUPON", 9, "'REG','ANU'");


        }

        static void fn_TransferirSocio()
        {
            string sEsquema = "";

            try
            {
                DataTable oPedidoDetalle = new DataTable();
                using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADO in('REG','APAT')  AND TRANSFERIDOEECC='0' AND FORMAPAGO IN('CREDITO') ) or (FORMAPAGO IN('CONTADO') AND ESTADOCUPON='PAG')   ", sConexion))
                {
                    //a Datatable to store records 
                    //now im going to fetch data
                    adp.Fill(oPedidoDetalle);//all the data in OracleAdapter will be filled into Datatable 

                }

                //Console.WriteLine("Cantidad Registros " + oPedidoDetalle.Rows.Count);
                //Console.ReadLine();

                if (oPedidoDetalle != null)
                {
                    foreach (DataRow oRows in oPedidoDetalle.Rows)
                    {
                        Console.WriteLine("Entro en transferencia");

                        oDbGeneric = new DBCGeneric();
                        oDbGeneric.fn_AdicionarObjeto("PA_EECC_MACRO_Adicionar",
                            oRows["SOCIO"].ToString(),
                            oRows["NumeroDocumetoSocio"].ToString(),
                            oRows["SocioCodigo"].ToString(),
                            oRows["FECHAINGRESO"].ToString(),
                            oRows["FECHAAPROBACION"].ToString(),
                            oRows["IMPORTEORDENPEDIDOORACLE"].ToString(),
                        "PENDIENTE",
                            "Ingresado x el módulo OP - " + oRows["FECHAINGRESO"].ToString(),
                            oRows["NUMEROORDENPEDIDOORACLE"].ToString(),
                            oRows["NUMEROCUPON"].ToString()
                            );

                        fn_Registrar("UPDATE " + sEsquema + "\"PEDIDODETALLE\" SET  TRANSFERIDOEECC='1' " +
                          " WHERE   PKID='" + oRows["IDPEDIDODETALLE"].ToString() + "' "
                        );

                    }


                }


                //fn_EjecutarMacro();

                /*
            fn_Registrar("INSERT INTO " + sEsquema + "\"PRUEBA_P\" (PKID, DESCRIPCION) VALUES( " +
         " '" + "1" + "'" +
        ", '" + "PRUEBA DE INSERCION" + "' )"
            );
                */

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //Console.ReadLine();
            }


        }

        static string ConvertToDate(string pFecha)
        {
            string sFecha = "";

            try
            {
                sFecha = Convert.ToDateTime(pFecha).ToShortDateString();
            }
            catch (Exception ex)
            {
                sFecha = ""; 
            }

            return sFecha;
        }

        static void fn_CargarEECC_ORACLE()
        {
            string sEsquema = "";
            string sQuery = "";
            string IDSocioNegocio = "";
            string IDTIPOSERVICIO = "";
            int pIDPK = 1;
            try
            {
                oDbGeneric = new DBCGeneric();

                fn_Registrar("DELETE FROM " + sEsquema + "W_EECC ");

                foreach (DataRow oRows in fn_ObtenerResultado("SELECT 'PEND' CodigoEstado,'PENDIENTE' ESTADO, 'DEMONIO' AS USUARIO, 1 IDCanal,82 IDMoneda,fech_archivo,canal,socio,RUC,CODIGO,NFACTURAS,FECHAVCTO,IMPORTESOLES,SALDOPENDIENTE,ESTADO FROM " + sEsquema + " EECC_EH_PEND").Rows)
                {
                    foreach (DataRow oSocioNegocio in fn_ObtenerResultado("select PKID,SOCIOD,SOCIOA,FORMAPAGO,TIPOSOCIO,TIPODOCUMENTO,NUMERODOCUMENTO,DIRECCION,CANAL,REQUIERESOCIOCAMPAÑA,ACTIVO,LINEACREDITOMN,CODIGO,IDTIPODOCUMENTO,IDCANAL,IDFORMAPAGO,IDTipoSocio,IDSocioRelacionado from " + sEsquema + "VIEW_SOCIO_LISTAR WHERE NUMERODOCUMENTO='" + oRows["RUC"] + "' AND IDSOCIORELACIONADO=0 ").Rows)
                    {
                        IDSocioNegocio = oSocioNegocio["PKID"].ToString();
                    }
                    Console.WriteLine("PASO SOCIONEOCIO");
                    //Console.ReadLine();

                    foreach (DataRow oSocioNegocio in fn_ObtenerResultado("select * from " + sEsquema + "TABLA_P WHERE ABREVIATURA='" + oRows["CODIGO"].ToString().Trim() + "' AND IDTIPOTABLA=13  ").Rows)
                    {
                        IDTIPOSERVICIO = oSocioNegocio["PKID"].ToString();
                    }
                    Console.WriteLine("PASO TABLA CODIGO");
                    //Console.ReadLine();

                    fn_Registrar("INSERT INTO " + sEsquema + "w_EECC (PKID,FECHATRANSFERENCIA,IDCANAL,IDSOCIO,IDTIPOSERVICIO,NUMEROFACTURA,FECHAVENCIMIENTO,IDMONEDA,IMPORTE,CODIGOESTADO,ESTADO,USUARIO,SALDOPENDIENTE) " +
                        " VALUES('" + pIDPK + "' ," +
                          "'" + DateTime.Now + "' ," +
                          "'" + oRows["IDCanal"] + "' ," +
                          "'" + IDSocioNegocio + "' ," +
                          "'" + IDTIPOSERVICIO + "' ," +
                          "'" + oRows["NFACTURAS"] + "' ," +
                          "'" + ConvertToDate(oRows["FECHAVCTO"].ToString()) + "' ," +
                          "'" + oRows["IDMONEDA"] + "' ," +

                          // IMPORTE,CODIGOESTADO,ESTADO,USUARIO,SALDOPENDIENTE
                          "'" + oRows["IMPORTESOLES"] + "' ," +
                          "'" + oRows["CODIGOESTADO"] + "' ," +
                          "'" + oRows["ESTADO"] + "' ," +
                          "'" + oRows["USUARIO"] + "' ," +
                          "'" + oRows["SALDOPENDIENTE"] + "' )");

                    pIDPK++;

                }



            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.ReadLine();
            }


        }

        static void fn_CargarEECC_SQL()
        {
            string sEsquema = "";
            string sQuery = "";
            string IDSocioNegocio = "";
            string IDTIPOSERVICIO = "";
            try
            {
                oDbGeneric = new DBCGeneric();

                fn_Registrar("DELETE FROM " + sEsquema + "W_EECC ");
                 
                foreach (DataRow oRows in oDbGeneric.fn_ObtenerResultado("PA_EECC_EH_PEND").Rows)
                {
                    foreach (DataRow oSocioNegocio in fn_ObtenerResultado("select PKID,SOCIOD,SOCIOA,FORMAPAGO,TIPOSOCIO,TIPODOCUMENTO,NUMERODOCUMENTO,DIRECCION,CANAL,REQUIERESOCIOCAMPAÑA,ACTIVO,LINEACREDITOMN,CODIGO,IDTIPODOCUMENTO,IDCANAL,IDFORMAPAGO,IDTipoSocio,IDSocioRelacionado from " + sEsquema + "VIEW_SOCIO_LISTAR WHERE NUMERODOCUMENTO='" + oRows["NumeroDocumentoSN"] + "'").Rows)
                    {
                        IDSocioNegocio = oSocioNegocio["PKID"].ToString();
                    }
                    Console.WriteLine("PASO SOCIONEOCIO");
                    //Console.ReadLine();

                    foreach (DataRow oSocioNegocio in fn_ObtenerResultado("select * from " + sEsquema + "TABLA_P WHERE ABREVIATURA='" + oRows["TIPOSERVICIO"].ToString().Trim() + "' AND IDTIPOTABLA=13  ").Rows)
                    {
                        IDTIPOSERVICIO = oSocioNegocio["PKID"].ToString();
                    }
                    Console.WriteLine("PASO TABLA CODIGO");
                    //Console.ReadLine();

                    fn_Registrar("INSERT INTO " + sEsquema + "w_EECC (PKID,FECHATRANSFERENCIA,IDCANAL,IDSOCIO,IDTIPOSERVICIO,NUMEROFACTURA,FECHAVENCIMIENTO,IDMONEDA,IMPORTE,CODIGOESTADO,ESTADO,USUARIO,SALDOPENDIENTE) " +
                        " VALUES('" + oRows["PKID"] + "' ," +
                          "'" + ConvertToDate(DateTime.Now.ToShortDateString()) + "' ," +
                          "'" + oRows["IDCanal"] + "' ," +
                          "'" + IDSocioNegocio + "' ," +
                          "'" + IDTIPOSERVICIO + "' ," +
                          "'" + oRows["NUMEROFACTURA"] + "' ," +
                          "'" + ConvertToDate( oRows["FECHAVENCIMIENTO"].ToString())   + "' ," +
                          "'" + oRows["IDMONEDA"] + "' ," +

                          // IMPORTE,CODIGOESTADO,ESTADO,USUARIO,SALDOPENDIENTE
                          "'" + oRows["IMPORTE"] + "' ," +
                          "'" + oRows["CODIGOESTADO"] + "' ," +
                          "'" + oRows["ESTADO"] + "' ," +
                          "'" + oRows["USUARIO"] + "' ," +
                          "'" + oRows["SALDOPENDIENTE"] + "' )" );

                }



            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.ReadLine();
            }


        }

        static DataTable fn_ObtenerResultado(string pQuery)
        {
            sConexion = ConfigurationManager.ConnectionStrings["cnxOracle"].ConnectionString;
            DataTable dt = new DataTable();

            using (OracleDataAdapter adp = new OracleDataAdapter(pQuery, sConexion))
            {
                adp.SelectCommand.CommandTimeout = 120;
                //adp.comma.CommandTimeout = 1190430;
                //a Datatable to store records 
                //now im going to fetch data
                adp.Fill(dt);//all the data in OracleAdapter will be filled into Datatable 
            }
              
            return dt;
        }

        static DataTable fn_ObtenerResultadoShadow(string pQuery)
        {
            sConexion = ConfigurationManager.ConnectionStrings["cnxOracle_shadow"].ConnectionString;
            DataTable dt = new DataTable();

            using (OracleDataAdapter adp = new OracleDataAdapter(pQuery, sConexion))
            {
                adp.SelectCommand.CommandTimeout = 120;
                //a Datatable to store records 
                //now im going to fetch data
                adp.Fill(dt);//all the data in OracleAdapter will be filled into Datatable 
            }

            return dt;
        }


        static void fn_EnviarCorreoCupon(string pTipoFiltro, int pIDCorreo, string sCodigoPedido)
        {
            string sEsquema = "";
            string NUMEROCUPON = "";
            try
            {
                //CORREOENVIADOAPROBACION1
                //CORREOENVIADO
                DataTable oPedidoDetalle = new DataTable();

                ///using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADO in('REG','APAT')  AND TRANSFERIDOEECC='0' AND FORMAPAGO IN('CREDITO') ) or (FORMAPAGO IN('CONTADO') AND ESTADOCUPON='PAG')   ", sConexion))
                using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio, IDSOCIO from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADOPEDIDO in(" + sCodigoPedido + ")  AND " + pTipoFiltro + " IS NULL AND  CODIGOESTADOCUPON NOT IN('PEND')  )   ", sConexion))
                {
                    //a Datatable to store records 
                    //now im going to fetch data
                    adp.Fill(oPedidoDetalle);//all the data in OracleAdapter will be filled into Datatable 

                }

                Console.WriteLine("Cantidad Registros " + oPedidoDetalle.Rows.Count);
                //Console.ReadLine();
                string sUsuario = "";
                if (oPedidoDetalle != null)
                {
                    foreach (DataRow oRows in oPedidoDetalle.Rows)
                    {
                        List<PedidoEntelDetalle> oLista = new List<PedidoEntelDetalle>();

                        foreach (DataRow oRows2 in fn_BuscarPo(oRows["IDPEDIDODETALLE"].ToString()).Rows)
                        {
                            PedidoEntelDetalle oPedidoEntelDetalle = new PedidoEntelDetalle();

                            oPedidoEntelDetalle.ESTADOCUPON = oRows2["ESTADOCUPON"].ToString();
                            oPedidoEntelDetalle.NUMEROCUPON = oRows2["NUMEROCUPON"].ToString();
                            oPedidoEntelDetalle.NUMEROOP = oRows2["NUMEROORDENPEDIDOORACLE"].ToString();
                            oPedidoEntelDetalle.FORMAPAGO = oRows2["FORMAPAGO"].ToString();
                            oPedidoEntelDetalle.IMPORTEOP = Convert.ToDecimal(oRows2["IMPORTEORDENPEDIDOORACLE"]);
                            oPedidoEntelDetalle.SOCIO = oRows2["SOCIO"].ToString();
                            oPedidoEntelDetalle.IDSOCIO = oRows2["IDSOCIO"].ToString();
                            sUsuario = oRows2["USUARIO"].ToString();
                            NUMEROCUPON = oRows2["NUMEROCUPON"].ToString();
                            //oPedidoEntelDetalle.COMENTARIOSOLICITUDEXCEPCION = oRows2["COMENTARIORECHAZO"].ToString();
                            //sComentarioRechazo = oPedidoEntelDetalle.COMENTARIOSOLICITUDEXCEPCION;
                            oLista.Add(oPedidoEntelDetalle);
                        }

                        BOCGeneric oBoGeneric = new BOCGeneric();

                        string sIDUsuario = "";
                        foreach (DataRow oRows2 in oBoGeneric.fn_ObtenerResultado("[SEGURIDAD].[Pa_Usuario_BuscarxUsuario]", sUsuario).Rows)
                        {
                            sIDUsuario = oRows2["ID"].ToString();
                        }


                        //string sIDUsuario = "97";
                        //bool CorreoEnviado = false;
                        bool CorreoEnviado = fn_EnvioCorreo(oLista, pIDCorreo, sIDUsuario, "");

                        if (CorreoEnviado)
                        {

                            fn_Registrar("UPDATE " + sEsquema + "\"PEDIDODETALLE\" SET  " + pTipoFiltro + "='1' " +
                              " WHERE   PKID='" + oRows["IDPEDIDODETALLE"].ToString() + "' "
                            );

                        }


                    }


                }


                //fn_EjecutarMacro();

                /*
            fn_Registrar("INSERT INTO " + sEsquema + "\"PRUEBA_P\" (PKID, DESCRIPCION) VALUES( " +
         " '" + "1" + "'" +
        ", '" + "PRUEBA DE INSERCION" + "' )"
            );
                */

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.ReadLine();
            }


        }

        static void fn_EnviarCorreo(string pTipoFiltro,int pIDCorreo,string sCodigoPedido,string pCondicional="")
        {
            string sEsquema = "";

            try
            {
                //CORREOENVIADOAPROBACION1
                //CORREOENVIADO
                DataTable oPedidoDetalle = new DataTable();

                ///using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADO in('REG','APAT')  AND TRANSFERIDOEECC='0' AND FORMAPAGO IN('CREDITO') ) or (FORMAPAGO IN('CONTADO') AND ESTADOCUPON='PAG')   ", sConexion))
                using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio, IDSOCIO from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADOPEDIDO in('"+ sCodigoPedido+ "')  AND " + pTipoFiltro + " IS NULL )   " + pCondicional , sConexion))
                {
                    //a Datatable to store records 
                    //now im going to fetch data
                    adp.Fill(oPedidoDetalle);//all the data in OracleAdapter will be filled into Datatable 

                }

                Console.WriteLine("Cantidad Registros " + oPedidoDetalle.Rows.Count);
                //Console.ReadLine();
                string sUsuario = "";
                if (oPedidoDetalle != null)
                {
                    foreach (DataRow oRows in oPedidoDetalle.Rows)
                    {
                        List<PedidoEntelDetalle> oLista = new List<PedidoEntelDetalle>();

                        Console.WriteLine("Llego 2 " );

                        foreach (DataRow oRows2 in fn_BuscarPo(oRows["IDPEDIDODETALLE"].ToString() ).Rows)
                        {
                            PedidoEntelDetalle oPedidoEntelDetalle = new PedidoEntelDetalle();

                            oPedidoEntelDetalle.NUMEROOP = oRows2["NUMEROORDENPEDIDOORACLE"].ToString();
                            oPedidoEntelDetalle.FORMAPAGO = oRows2["FORMAPAGO"].ToString();
                            oPedidoEntelDetalle.IMPORTEOP = Convert.ToDecimal(oRows2["IMPORTEORDENPEDIDOORACLE"]);
                            oPedidoEntelDetalle.SOCIO = oRows2["SOCIO"].ToString();
                            oPedidoEntelDetalle.NUMEROCUPON = oRows2["NUMEROCUPON"].ToString();
                            oPedidoEntelDetalle.IDSOCIO = oRows2["IDSOCIO"].ToString();

                            sUsuario = oRows2["USUARIO"].ToString();

                            //oPedidoEntelDetalle.COMENTARIOSOLICITUDEXCEPCION = oRows2["COMENTARIORECHAZO"].ToString();
                            //sComentarioRechazo = oPedidoEntelDetalle.COMENTARIOSOLICITUDEXCEPCION;
                            oLista.Add(oPedidoEntelDetalle);

                            Console.WriteLine("oPedidoEntelDetalle.NUMEROOP" + oPedidoEntelDetalle.NUMEROOP);

                        }

                        BOCGeneric oBoGeneric = new BOCGeneric();

                        string sIDUsuario = "";
                        foreach (DataRow oRows2 in oBoGeneric.fn_ObtenerResultado("[SEGURIDAD].[Pa_Usuario_BuscarxUsuario]", sUsuario).Rows)
                        {
                            sIDUsuario = oRows2["ID"].ToString();
                        }
                        
                        Console.WriteLine("sIDUsuario" + sIDUsuario);


                        //string sIDUsuario = "97";

                        //bool CorreoEnviado = false;
                        bool CorreoEnviado = fn_EnvioCorreo(oLista, pIDCorreo, sIDUsuario, "");

                        Console.WriteLine("CorreoEnviado " + CorreoEnviado);
                        //Console.ReadLine();

                        if (CorreoEnviado) {  

                        fn_Registrar("UPDATE " + sEsquema + "\"PEDIDODETALLE\" SET  "+ pTipoFiltro + "='1' " +
                          " WHERE   PKID='" + oRows["IDPEDIDODETALLE"].ToString() + "' "
                        );



                             }


                }


                }


                //fn_EjecutarMacro();

                /*
            fn_Registrar("INSERT INTO " + sEsquema + "\"PRUEBA_P\" (PKID, DESCRIPCION) VALUES( " +
         " '" + "1" + "'" +
        ", '" + "PRUEBA DE INSERCION" + "' )"
            );
                */

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //Console.ReadLine();
            }


        }


        static bool fn_EnvioCorreo(List<PedidoEntelDetalle> ListaPedidoEntelDetalle, int pIDCorreo, string pIDUsuario, string pObservacion
            , string pEstadoCupon = ""
            )
        {
            bool CorreoEnviado = false;

            BOCCorreo oBoCorreo = new BOCCorreo();

            //MDDTOEntities.Maestros.Comprobante oComprobante = new MDDTOEntities.Maestros.Comprobante();
            swEnvioCorreo.Comprobante oComprobante = new swEnvioCorreo.Comprobante();

            oComprobante.ListaComprobanteDetalle = new List<swEnvioCorreo.ComprobanteDetalle>();
            //oComprobante.ListaComprobanteDetalle = new List<MDDTOEntities.Maestros.ComprobanteDetalle>();

            //new List<MDDTOEntities.Maestros.ComprobanteDetalle>();
            //List<string> oLista = new List<string>();

            swEnvioCorreo.ArrayOfString oLista = new swEnvioCorreo.ArrayOfString();

            string NumeroOrdenCompra = "";
            int iCount = 1;
            int TotalPedido = ListaPedidoEntelDetalle.Count;
            decimal ImporteTotalPedido = ListaPedidoEntelDetalle.Sum(x => x.IMPORTEOP);

            Console.WriteLine("ImporteTotalPedido: " + ImporteTotalPedido);
            Console.WriteLine("ListaPedidoEntelDetalle: " + ListaPedidoEntelDetalle.Count);

            oComprobante.ObservacionComprobante = pObservacion;
            string iIDSocio = "0";

            
            foreach (PedidoEntelDetalle item in ListaPedidoEntelDetalle)
            {
               swEnvioCorreo.ComprobanteDetalle oComprobanteDetalle = new swEnvioCorreo.ComprobanteDetalle();
                // MDDTOEntities.Maestros.ComprobanteDetalle oComprobanteDetalle = new MDDTOEntities.Maestros.ComprobanteDetalle();

                Console.WriteLine("item.NUMEROOP: " + item.NUMEROOP);

                if (iCount == TotalPedido)
                    NumeroOrdenCompra = item.NUMEROOP;
                else
                    NumeroOrdenCompra = item.NUMEROOP + ",";

                iIDSocio = item.IDSOCIO;
                oComprobanteDetalle.CodigoAlterno = item.FORMAPAGO;
                oComprobanteDetalle.NumeroDocumentoReferencia = item.NUMEROOP;
                oComprobanteDetalle.ImporteDocumentoVentaDetalle = item.IMPORTEOP;
                oComprobanteDetalle.AlmacenDescripcion = item.NUMEROCUPON;

                oComprobante.EstadoxEntregar = item.ESTADOCUPON;
                oComprobante.NumeracionReferencial1 = item.NUMEROCUPON;

                oComprobante.ListaComprobanteDetalle.Add(oComprobanteDetalle);

                oComprobante.RazonSocialCliente = item.SOCIO;
                iCount++;
            }
            oComprobante.NumeroOrdenCompra = NumeroOrdenCompra;
            oComprobante.Total = ImporteTotalPedido;
            //oComprobante.EstadoxEntregar = pEstadoCupon;

            string sUsuario = "";

            BOCGeneric oBoGeneric = new BOCGeneric();
            string sCorreoDestinatario = "";
            foreach (DataRow oRows in oBoGeneric.fn_ObtenerResultado("seguridad.Pa_Usuario_Buscar", pIDUsuario).Rows)
            {
                sCorreoDestinatario = oRows["Correo"].ToString();
            }


            Console.WriteLine("sCorreoDestinatario" + sCorreoDestinatario);
            if (sCorreoDestinatario != null)
                oLista.Add(sCorreoDestinatario);


            bool Res = false;


            string sMensaje = "";
            try
            {
                swEnvioCorreo.swComprobanteSoapClient oEnvioCorreo = new swEnvioCorreo.swComprobanteSoapClient();
                //BOCCorreo oCCorreo= new BOCCorreo();    
                Console.WriteLine("pIDCorreo " + pIDCorreo);

                string sTO_CORREO = "";
                string sCC_CORREO = "";

                string []sTO_CORREO_ARRAY ;
                string []sCC_CORREO_ARRAY ;

                if (oLista != null)
                {
                    foreach (DataRow oRows in 
                        fn_ObtenerResultado("SELECT * FROM SOCIO where PKID = '"+ iIDSocio  + "'").Rows)
                    {
                        sTO_CORREO_ARRAY = oRows["TO_CORREO"].ToString().Split(';');
                        sCC_CORREO_ARRAY = oRows["CC_CORREO"].ToString().Split(';');

                        if(sTO_CORREO_ARRAY.Length> 1) {  
                        foreach(var tocorreo in sTO_CORREO_ARRAY)
                        {
                            oLista.Add (tocorreo);
                        }
                   
                        if (sCC_CORREO_ARRAY.Length > 1)
                        {
                            foreach (var tocorreo in sCC_CORREO_ARRAY)
                            {
                                oLista.Add(tocorreo);
                            }
                        }
                    }


                    }

                    foreach (var pRes in oLista)
                    {

                        Console.WriteLine(pRes);
                    }

                }


                sMensaje = oEnvioCorreo.fn_EnviarCorreoPedido(oComprobante, pIDCorreo, oLista);
                //Res= oCCorreo.fn_EnviarConfiguracionPedido(oComprobante, pIDCorreo, oLista,"");
                //sMensaje =oEnvioCorreo.fn_EnviarCorreoPedido(oComprobante, pIDCorreo, oLista);

                //Res = true;
            }
            catch (Exception ex)
            {
                sMensaje = ex.Message;
            }

            Console.WriteLine("sMensaje " + sMensaje );
            //Console.ReadLine();


            if (sMensaje == "")
                Res = true;

            return Res; // oBoCorreo.fn_EnviarConfiguracionPedido(oComprobante, pIDCorreo, oLista, "");

        }


        public static DataTable fn_BuscarPo(string pIDDetalle)
        {
            string sConexion = ConfigurationManager.ConnectionStrings["cnxOracle"].ConnectionString;
            DataTable dt = new DataTable();

            using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,IDSOCIO from " + sEsquema + "V_PEDIDODETALLE_LISTAR WHERE IDPEDIDODETALLE ='" + pIDDetalle + "'", sConexion))
            {
                //a Datatable to store records 
                //now im going to fetch data
                adp.Fill(dt);//all the data in OracleAdapter will be filled into Datatable 

            }


            return dt;
        }

        static void fn_TransferirEECC() {
            string sEsquema = "";

            try
            {
                DataTable oPedidoDetalle = new DataTable();
                ///using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADO in('REG','APAT')  AND TRANSFERIDOEECC='0' AND FORMAPAGO IN('CREDITO') ) or (FORMAPAGO IN('CONTADO') AND ESTADOCUPON='PAG')   ", sConexion))
                using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADO in('APAT')  AND TRANSFERIDOEECC='0' )   ", sConexion))
                {
                    //a Datatable to store records 
                    //now im going to fetch data
                    adp.Fill(oPedidoDetalle);//all the data in OracleAdapter will be filled into Datatable 

                }

                //Console.WriteLine("Cantidad Registros " + oPedidoDetalle.Rows.Count);
                //Console.ReadLine();

                if (oPedidoDetalle != null)
                {
                    foreach (DataRow oRows in oPedidoDetalle.Rows)
                    {
                        Console.WriteLine("Entro en transferencia");

                        oDbGeneric = new DBCGeneric();
                        oDbGeneric.fn_AdicionarObjeto("PA_EECC_MACRO_Adicionar",
                            oRows["SOCIO"].ToString(),
                            oRows["NumeroDocumetoSocio"].ToString(),
                            oRows["SocioCodigo"].ToString(),
                            oRows["FECHAINGRESO"].ToString(),
                            oRows["FECHAAPROBACION"].ToString(),
                            oRows["IMPORTEORDENPEDIDOORACLE"].ToString(),
                        //"PENDIENTE",
                        oRows["ESTADOCUPON"].ToString(),
                            "Ingresado x el módulo OP WEB - " + oRows["FECHAINGRESO"].ToString(),
                            oRows["NUMEROORDENPEDIDOORACLE"].ToString(),
                            oRows["NUMEROCUPON"].ToString()
                            );

                        fn_Registrar("UPDATE " + sEsquema + "\"PEDIDODETALLE\" SET  TRANSFERIDOEECC='1' " +
                          " WHERE   PKID='" + oRows["IDPEDIDODETALLE"].ToString() + "' "
                        );

                    }


                }


                //fn_EjecutarMacro();

                /*
            fn_Registrar("INSERT INTO " + sEsquema + "\"PRUEBA_P\" (PKID, DESCRIPCION) VALUES( " +
         " '" + "1" + "'" +
        ", '" + "PRUEBA DE INSERCION" + "' )"
            );
                */

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //Console.ReadLine();
            }


        }


        static void fn_TransferirEECC_ORACLE()
        {
            string sEsquema = "";

            try
            {

                //fn_Registrar("DELETE FROM " + sEsquema + "EECC_MACRO ");



                DataTable oPedidoDetalle = new DataTable();
                ///using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADO in('REG','APAT')  AND TRANSFERIDOEECC='0' AND FORMAPAGO IN('CREDITO') ) or (FORMAPAGO IN('CONTADO') AND ESTADOCUPON='PAG')   ", sConexion))
                using (OracleDataAdapter adp = new OracleDataAdapter("SELECT IDPEDIDO,IDPEDIDODETALLE,FECHAINGRESO,CANAL,SOCIO ,SOCIOCODIGO,LINEACREDITO,IDFORMAPAGO,FORMAPAGO,DEUDAVENCIDA,DISPONIBLE,NUMEROORDENPEDIDOORACLE, IMPORTEORDENPEDIDOORACLE ,NUMEROCUPON,ESTADOCUPON,ESTADORIESGO,CODIGOESTADOPEDIDO,ESTADOPEDIDO,IDOPERACION,USUARIO,COMENTARIORECHAZO,FECHAAPROBACION,NumeroDocumetoSocio from " + sEsquema + "V_PEDIDODETALLE_LISTAR2 where (CODIGOESTADO in('APAT')  AND TRANSFERIDOEECC='0' )   ", sConexion))
                {
                    //a Datatable to store records 
                    //now im going to fetch data
                    adp.Fill(oPedidoDetalle);//all the data in OracleAdapter will be filled into Datatable 

                }

                //Console.WriteLine("Cantidad Registros " + oPedidoDetalle.Rows.Count);
                //Console.ReadLine();
                int iIDPK = 1;
                if (oPedidoDetalle != null)
                {
                    foreach (DataRow oRows in oPedidoDetalle.Rows)
                    {
                        Console.WriteLine("Entro en transferencia");

                        try
                        {

                        oDbGeneric = new DBCGeneric();
                        fn_Registrar("INSERT INTO " + sEsquema + "EECC_MACRO  (ID,SOCIONEGOCIO,NUMERODOCUMENTOSOCIONEGOCIO,CODIGOTIPO,FECHAEMISION,FECHAAPROBACION,IMPORTESOLES,ESTADO,OPOBSERVACIONES,NROORDEN,NROCUPON) " + 
                            " VALUES( '" +iIDPK + "'," +
                            "'" + oRows["SOCIO"].ToString() + "'," +

                            "'" + oRows["NumeroDocumetoSocio"].ToString() + "'," +
                            "'" + oRows["SocioCodigo"].ToString() + "'," +
                            "'" + ConvertirFormatoFechaExcel(oRows["FECHAINGRESO"].ToString()) + "'," +
                            "'" + ConvertirFormatoFechaExcel(oRows["FECHAAPROBACION"].ToString()) + "'," +
                            "'" + oRows["IMPORTEORDENPEDIDOORACLE"].ToString() + "'," +
                        //"PENDIENTE",
                        "'" + oRows["ESTADOCUPON"].ToString() + "'," +
                        "'" + "Ingresado x el módulo OP WEB - " + ConvertirFormatoFechaExcel(oRows["FECHAINGRESO"].ToString()) + "'," +
                            "'" + oRows["NUMEROORDENPEDIDOORACLE"].ToString() + "'," +
                            "'" +  oRows["NUMEROCUPON"].ToString() + "' )" 
                            );


                        }
                        catch (Exception ex)
                        {
                             Console.WriteLine (ex.ToString());
                        }

                        fn_Registrar("UPDATE " + sEsquema + "\"PEDIDODETALLE\" SET  TRANSFERIDOEECC='1' " +
                          " WHERE   PKID='" + oRows["IDPEDIDODETALLE"].ToString() + "' "
                        );
                        iIDPK++;
                    }


                }


                //fn_EjecutarMacro();

                /*
            fn_Registrar("INSERT INTO " + sEsquema + "\"PRUEBA_P\" (PKID, DESCRIPCION) VALUES( " +
         " '" + "1" + "'" +
        ", '" + "PRUEBA DE INSERCION" + "' )"
            );
                */

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                //Console.ReadLine();
            }


        }


      static  string ConvertirFormatoFechaExcel(string pFecha)
        {
            string sFechaReturn = "";

            try
            {
                string[] sArrayFecha = pFecha.Split('/');

                sFechaReturn = sArrayFecha[0] + "/" + sArrayFecha[1] + "/" + (Convert.ToInt32(sArrayFecha[2]) - 2000);

            }
            catch (Exception ex)
            {
                sFechaReturn = "";
            }
            
            return sFechaReturn;
        }

        static void fn_EjecutarMacro()
        {
            Process scriptProc = new Process();
            scriptProc.StartInfo.FileName = @"cscript";
            scriptProc.StartInfo.WorkingDirectory = @"D:\SharePoint\Entel Peru S.A\EntelDrive Canal Indirecto y Fraudes - CORREOS_CONFIRMACION\"; //<---very important 
            scriptProc.StartInfo.Arguments = "//B //Nologo EnvioConfOP.vbs";
            scriptProc.StartInfo.WindowStyle = ProcessWindowStyle.Hidden; //prevent console window from popping up
            scriptProc.Start();
            scriptProc.WaitForExit(); // <-- Optional if you want program running until your script exit
            scriptProc.Close();

        }


        static string fn_Registrar(string pQUERY)
        {
            Console.WriteLine("sConexion: "+ sConexion);


            using (var con = new Oracle.ManagedDataAccess.Client.OracleConnection(sConexion))
            {
                con.Open();

                OracleParameter id = new OracleParameter();
                id.OracleDbType = OracleDbType.Varchar2;
                id.Value = DateTime.Now.ToLongDateString();

                Console.WriteLine("pQUERY : " + pQUERY);
                // create command and set properties
                OracleCommand cmd = con.CreateCommand();
                cmd.CommandText = pQUERY;  //"INSERT INTO BULKINSERTTEST (ID, NAME, ADDRESS) VALUES (:1, :2, :3)";
                                           //cmd.ArrayBindCount = ids.Length;
                                           //cmd.Parameters.Add(id);
                                           //cmd.Parameters.Add(name);
                                           //cmd.Parameters.Add(address);
                cmd.ExecuteNonQuery();

            }

            Console.WriteLine("INSERTADO CORRECTAMENTE: ");

            return "1";
        }

    }
}

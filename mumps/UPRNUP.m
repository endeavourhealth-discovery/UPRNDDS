UPRNUP ; ; 11/13/19 11:41am
  
SETUP ;
 S ^%W(17.6001,125,0)="GET"
 S ^%W(17.6001,125,1)="api/getadr"
 S ^%W(17.6001,125,2)="GET^UPRNUP"
 S ^%W(17.6001,"B","GET","api/getadr","GET^UPRNUP",125)=""
 
 S ^%W(17.6001,126,0)="POST"
 S ^%W(17.6001,126,1)="api/postadr"
 S ^%W(17.6001,126,2)="POST^UPRNUP"
 S ^%W(17.6001,"B","POST","api/postadr","POST^UPRNUP",126)=""
 
 QUIT
 
GET(result,arguments) 
 n page,uuid
 S page=$get(arguments("page"))
 S uuid=$get(arguments("uuid")) 
 K ^TMP($J)
 
 i page'="",uuid'="" do
 .s (org,id)="",count=1 
 .f  s org=$o(^UPRNBATCH(uuid,page,org)) q:org=""  d
 ..f  s id=$o(^UPRNBATCH(uuid,page,org,id)) q:id=""  d
 ...S uprn=^(id)
 ...S ^TMP($J,count)=org_","_id_","_uprn_"`"
 ...S count=$i(count)
 ...q
 ..quit
 .quit
 
 set result("mime")="text/plain, */*"
 set result=$na(^TMP($j))
 QUIT
 
POST(arguments,body,result) 
 N ZZ,ZI,ADDS,ZPAGE,ZC
 
 ;M ^BODY=body
 
 S ^TMP($J,1)="ok"
 S H=+$H,T=$P($H,",",2)
 
 S ZI="",ZPAGE=1,ZC=0,ADDS=""
 F  S ZI=$O(body(ZI)) Q:ZI=""  D
 .S ADDS=ADDS_body(ZI) 
 .QUIT
 
 S UUID=$P($P(ADDS,"`"),"~",9)
 S ZPAGE=$O(^UPRNBATCH(UUID,""),-1)+1
 
 F ZZ=1:1:$L(ADDS,"`") D
 .S ADD=$P(ADDS,"`",ZZ)
 .I ADD="" QUIT
 .S ^PS($O(^PS(""),-1)+1)=ADD
 .S ADD1=$P(ADD,"~",1)
 .S ADD2=$P(ADD,"~",2)
 .S ADD3=$P(ADD,"~",3)
 .S ADD4=$P(ADD,"~",4)
 .S CITY=$P(ADD,"~",5)
 .S POSTCODE=$P(ADD,"~",6)
 .S ORGID=$P(ADD,"~",7)
 .S ADDID=$P(ADD,"~",8)
 .S UUID=$P(ADD,"~",9)
 .S ADREC=ADD1_","_ADD2_","_ADD3_","_ADD4_","_CITY_","_POSTCODE
 .D GETUPRN^UPRNMGR(ADREC,"","","",0,0)
 .S (JSON,UPRN,ALG,ADDQUAL)=""
 .i $d(^TUPRN($J,"MATCHED")) d
 ..S MATCHED=$$HD^STDDATE(+$H)_":"_$$HT^STDDATE($P($H,",",2)) 
 ..S ^UPRNBATCH(UUID,ZPAGE,ORGID,ADDID,"MATCHED")=$H_"~"_MATCHED
 ..K b
 ..D DECODE^VPRJSON($name(^temp($j,1)),$name(b),$name(err))
 ..set UPRN=$get(b("UPRN"))
 ..D META(.b)
 ..quit
 .S ^UPRNBATCH(UUID,ZPAGE,ORGID,ADDID)=UPRN
 .S ^UPRNBATCH(UUID,ZPAGE,ORGID,ADDID,"ADD")=ADREC
 .S ^UPRNBATCH(UUID,ZPAGE,ORGID,ADDID,"JSON")=$get(^temp($j,1))
 .I UPRN'="" do
 ..S COORD=$piece($get(^UPRN("U",UPRN)),"~",7)
 ..S LAT=$P(COORD,",",3),LONG=$P(COORD,",",4)
 ..S POINT=$P(COORD,",",3),X=$P(COORD,",",1),Y=$P(COORD,",",2) 
 ..S CLASS=$piece($get(^UPRN("CLASS",UPRN)),"~",1)
 ..S ^UPRNBATCH(UUID,ZPAGE,ORGID,ADDID,"COORD")=LAT_"~"_LONG_"~"_POINT_"~"_X_"~"_Y
 ..S ^UPRNBATCH(UUID,ZPAGE,ORGID,ADDID,"CLASS")=CLASS
 ..QUIT
 .S ZC=$I(C)
 .I ZC#10=0 S ZPAGE=ZPAGE+1
 .QUIT
 
 set result("mime")="text/html"
 set result=$na(^TMP($J))
 QUIT 1
RECON ;
 S (UUID,ZPAGE,ORGID,ADDID)=""
 K TOTS
 F  S UUID=$ORDER(^UPRNBATCH(UUID)) Q:UUID=""  DO
 .F  S ZPAGE=$O(^UPRNBATCH(UUID,ZPAGE)) Q:ZPAGE=""  DO
 ..F  S ORGID=$O(^UPRNBATCH(UUID,ZPAGE,ORGID)) Q:ORGID=""  DO
 ...F  S ADDID=$O(^UPRNBATCH(UUID,ZPAGE,ORGID,ADDID)) Q:ADDID=""  DO
 ....W !,ADDID
 ....S TOTS("ORG",ORGID)=$G(TOTS("ORG",ORGID))+1
 ....S TOTS("GRAND")=$G(TOTS("GRAND"))+1
 ....Q
 ...Q
 ..Q
 .Q
 ZWR TOTS
 QUIT
 
META(b) ;
 ; NATMATCH = Nature of Match
 ; ALG = Algorithm used
 ; QUAL = Qualifier
 ; QUALITY = quality indicator
 ; MATPATBUILD = Match pattern building
 ; MATPATFLAT = Match pattern flat
 ; MATPATNUMBER = Match pattern number
 ; MATPATPOSTCODE = Match pattern post code
 ; MATPATSTREET = Match pattern street
 set ALG=$get(b("Algorithm"))
 set QUAL=$get(b("Qualifier"))
 set MATPATBUILD=$get(b("Match_pattern","Building"))
 set MATPATFLAT=$get(b("Match_pattern","Flat"))
 set MATPATNUMBER=$get(b("Match_patrtern","Number"))
 set MATPATPSTCDE=$get(b("Match_patrtern","Postcode"))
 set MATPATSTRT=$get(b("Match_patrtern","Street"))
 set QUALITY=$get(b("Address_format"))
 set:MATPATBUILD'="" ^UPRMETA(UUID,ORGID,ADDID,"MATPATBUILD")=MATPATBUILD
 set:MATPATFLAT'="" ^UPRNMETA(UUID,ORGID,ADDID,"MATPATFLAT")=MATPATFLAT
 set:MATPATNUMBER'="" ^UPRNMETA(UUID,ORGID,ADDID,"MATPATNUMBER")=MATPATNUMBER
 set:MATPATPSTCDE'="" ^UPRNMETA(UUID,ORGID,ADDID,"MATPATPOSTCODE")=MATPATPSTCDE
 set:MATPATSTRT'="" ^UPRNMETA(UUID,ORGID,ADDID,"MATPATSTRT")=MATPATSTRT
 set:QUALITY'="" ^UPRNMETA(UUID,ORGID,ADDID,"QUALITY")=QUALITY
 set:QUAL'="" ^UPRNMETA(UUID,ORGID,ADDID,"QUALIFIER")=QUAL
 set:ALG'="" ^UPRNMETA(UUID,ORGID,ADDID,"ALG")=ALG
 Q

let $allCatalogs := 
	for $catalog in /Catalog order by $catalog/@timestamp descending return $catalog
	
let $newCatalog := $allCatalogs[1]
let $oldCatalog := $allCatalogs[2]

let $newCatalogStudies :=
	for $newStudy in $newCatalog/Study
		let $oldStudy := $oldCatalog/Study[@id=$newStudy/@id]
		let $status := if($oldStudy) then if($newStudy/@timestamp=$oldStudy/@timestamp) then "same" else "changed" else "new"
		return <Study id="{$newStudy/@id}" oldTimestamp="{$oldStudy/@timestamp}" timestamp="{$newStudy/@timestamp}" status="{$status}"/>


for $study in $newCatalogStudies
 return concat($study/@id,':',$study/@timestamp,'&#10;')
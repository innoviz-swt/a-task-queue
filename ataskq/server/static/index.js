const getColumn = (table, field) => {
    try {
        return table.getColumn(field);
    } catch (e) {
        return undefined;
    }
}

const init_table = (tabledata) => {
    //initialize table
    var table = new Tabulator("#table", {
        layout: "fitDataStretch",
        data: tabledata, //assign data to table
        autoColumns: true, //create columns from data field names
    });

    table.on("tableBuilt", () => {
        // edit job_id
        const job_id = getColumn(table, "job_id");
        if (job_id) {
            job_id.updateDefinition({ // Update the column definition
                formatter: "link",
                formatterParams: {
                    urlPrefix: "/custom_query/tasks_status/",
                }
            });
        }

        // edit targs
        const targs = getColumn(table, "targs");
        if (targs) {
            targs.updateDefinition({ // Update the column definition
                formatter: (cell, formatterParams, onRendered) => {
                    cell.getElement().classList.add("targs-cell");
                    return cell.getValue();
                }
            });
        }

    });
};

const myfetch = async (pathname, search) => {
    // handle /db/ prefix
    let url = pathname.replace('/db', '');
    url = '/api' + url;
    const response = await fetch(url + search);
    if (!response.ok) {
        const err = await response.json();
        throw err;
    }

    const data = await response.json();
    console.log('fetch', data);
    if (data){
        init_table(data);
    }

}

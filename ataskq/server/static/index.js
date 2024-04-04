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
    init_table(data);
}

(function () {
    const { pathname, search, hash } = window.location;
    console.table({ pathname, search })
    // update submit action
    const form = document.getElementById("tasks-status-go");
    form.addEventListener("submit", function (event) {
        // Prevent the default form submission behavior
        event.preventDefault();
        const inputValue = document.getElementById("tasks-status-go-input").value;
        const redirectUrl = form.getAttribute("action") + "/" + encodeURIComponent(inputValue);

        // Perform the redirect
        window.location.href = redirectUrl;
    });



    // update job id ] title
    let job_id;
    let title;
    if (pathname.startsWith("/custom_query/tasks_status/")) {
        job_id = pathname.split("/custom_query/tasks_status/")[1];
        title = 'tasks_status - job ' + job_id
    }
    else {
        job_id = new URLSearchParams(search).get('job_id');
        title = pathname.replace('/custom_query/', '').replace('/db/', '').replace('/', ' ')
        if (job_id){
            title += ` - job ${job_id}`
        }
    }

    if (job_id) {
        // update nav bar go
        document.getElementById("tasks-status-go-input").value = job_id;

        // update 'add job id elements'
        for (const dom of document.getElementsByClassName('add-job-id')) {
            const searchParams = new URLSearchParams();
            searchParams.append("job_id", job_id);
            dom.href += '?' + searchParams;
        };
    }

    // update title
    document.getElementsByClassName('nv-title')[0].innerHTML = title

    // fetch table from api
    myfetch(pathname, search).catch(error => {
        if (Array.isArray(error) && error.length == 1) {
            error = error[0];
        }
        error = JSON.stringify(error, null, 2);
        // todo: replace with notification popup
        alert(`Fetch failed. Error: ${error}`);
    });
}());

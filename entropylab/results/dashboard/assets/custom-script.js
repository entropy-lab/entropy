const selector_for_success_filter_container = ".dash-filter.column-5";
const id_of_new_success_filter = "success-filter-checklist";

waitForElm(".dash-filter.column-5").then((elm) => {
    reposition_success_filter()
});

function reposition_success_filter() {
    container = document.querySelector(selector_for_success_filter_container);
    if (container === null) {
        console.error("Couldn't find 'success' filter container")
    } else {
        removeChildren(container);
        new_filter = document.getElementById(id_of_new_success_filter);
        if (new_filter === null) {
            console.error("Couldn't find new 'success' filter checklist")
        } else {
            container.appendChild(new_filter)
        }
    }
}

function removeChildren(element) {
    while (element && element.firstChild) {
        element.removeChild(element.firstChild);
    }
}

// Source: https://stackoverflow.com/a/61511955/33404
function waitForElm(selector) {
    return new Promise(resolve => {
        if (document.querySelector(selector)) {
            return resolve(document.querySelector(selector));
        }

        const observer = new MutationObserver(mutations => {
            if (document.querySelector(selector)) {
                resolve(document.querySelector(selector));
                observer.disconnect();
            }
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    });
}

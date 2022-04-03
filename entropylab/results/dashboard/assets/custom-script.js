window.setTimeout(() => {work()}, 1000);

function work() {
/*
  th = document.querySelector('*[data-dash-column="success"] div');
  console.log(th);
  filter_headers = document.getElementsByClassName("dash-filter column-5");
  if (filter_headers.length != 1) {
    console.error("Couldn't find <th> element for success filter")
    return;
  }
  filter_header = filter_headers[0];
  */
  filter_header = document.querySelector(".dash-filter.column-5");
  removeChildren(filter_header);
  new_filter = document.getElementById("success-filter-checklist");
  if (new_filter === null) {
    console.error("Couldn't find 'success-filter-checklist' to reposition")
    return;
  }
  filter_header.appendChild(new_filter)
}

function removeChildren(element) {
  while (element.firstChild) {
    console.error("removing ", element.firstChild)
    element.removeChild(element.firstChild);
  }
}

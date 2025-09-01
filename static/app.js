const { useState, useEffect } = React;

function App() {
  const [customers, setCustomers] = useState([]);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const size = 10;

  useEffect(() => {
    fetch(`/api/customers?page=${page}&size=${size}`)
      .then((res) => res.json())
      .then((data) => {
        setCustomers(data.data);
        setTotal(data.total);
      });
  }, [page]);

  const totalPages = Math.ceil(total / size) || 1;

  return (
    React.createElement('div', null,
      React.createElement('h1', null, 'Customer List'),
      React.createElement('table', { className: 'table' },
        React.createElement('thead', null,
          React.createElement('tr', null,
            React.createElement('th', null, 'ID'),
            React.createElement('th', null, 'First Name'),
            React.createElement('th', null, 'Last Name'),
            React.createElement('th', null, 'Email')
          )
        ),
        React.createElement('tbody', null,
          customers.map(c => (
            React.createElement('tr', { key: c.customer_id },
              React.createElement('td', null, c.customer_id),
              React.createElement('td', null, c.first_name),
              React.createElement('td', null, c.last_name),
              React.createElement('td', null, c.email)
            )
          ))
        )
      ),
      React.createElement('div', { className: 'pagination' },
        React.createElement('button', { disabled: page <= 1, onClick: () => setPage(page - 1) }, 'Prev'),
        React.createElement('span', null, `${page} / ${totalPages}`),
        React.createElement('button', { disabled: page >= totalPages, onClick: () => setPage(page + 1) }, 'Next')
      )
    )
  );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(React.createElement(App));

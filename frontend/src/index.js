import React from 'react';
import ReactDOM from 'react-dom';
import * as Sentry from '@sentry/react';
import CodalabApp from './CodalabApp';
import * as serviceWorker from './serviceWorker';
import './css/imports.scss';

Sentry.init({
    dsn: process.env.REACT_APP_CODALAB_SENTRY_INGEST_URL,
    environment: process.env.REACT_APP_CODALAB_SENTRY_ENVIRONMENT,
});
console.log(process.env.REACT_APP_CODALAB_SENTRY_INGEST_URL);
ReactDOM.render(<CodalabApp />, document.getElementById('root'));

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: http://bit.ly/CRA-PWA
serviceWorker.unregister();

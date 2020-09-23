// test-utils.js
import React from 'react';
import { render } from '@testing-library/react';
import { CookiesProvider } from 'react-cookie';

import { MuiThemeProvider } from '@material-ui/core/styles';
import CodalabTheme from '../theme';

import '@testing-library/jest-dom/extend-expect';

import jqt from 'jquery.terminal';
jqt(window);

const AllTheProviders = ({ children }) => {
    return (
        <CookiesProvider>
            <MuiThemeProvider theme={CodalabTheme}>{children}</MuiThemeProvider>
        </CookiesProvider>
    );
};

const customRender = (ui, options) => render(ui, { wrapper: AllTheProviders, ...options });

// re-export everything
export * from '@testing-library/react';

// override render method
export { customRender as render };

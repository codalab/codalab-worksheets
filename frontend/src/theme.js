import { createMuiTheme } from '@material-ui/core/styles';

export default createMuiTheme({
    palette: {
        primary: {
            light: '#AAD4F6',
            main: '#3183C8',
            dark: '#2368A2',
            contrastText: '#EFF8FF',
        },
        secondary: {
            light: '#B8C4CF',
            main: '#8895A7',
            dark: '#5F6B7A',
            contrastText: '#EDF1F5',
        }
    },
    color: {
        primary: {
            darkest: '#194971',  // headings
            dark: '#2368A2',     // text
            base: '#3183C8',     // icons, buttons
            light: '#AAD4F6',    // outlines
            lightest: '#EFF8FF', // boxes
        },
        grey: {
            darkest: '#5F6B7A',
            dark: '#8895A7',
            base: '#B8C4CF',
            light: '#EDF1F5',
            lightest: '#F5F7FA',
        },
        teal: {
            darkest: '#1B655E',
            dark: '#2A9187',
            base: '#3CAEA3',
            light: '#A8EEEC',
            lightest: '#E7FFFE',
        },
        red: {
            darkest: '#881B1B',
            dark: '#B82020',
            base: '#DC3030',
            light: '#F4AAAA',
            lightest: '#FCE8E8',
        },
        yellow: {
            darkest: '#8C6D1F',
            dark: '#CAA53D',
            base: '#F4CA64',
            light: '#FDF3D7',
            lightest: '#FFFCF4',
        },
        green: {
            darkest: '#187741',
            dark: '#249D57',
            base: '#38C172',
            light: '#A8EEC1',
            lightest: '#E3FCEC',
        },
    },
    typography: {
        htmlFontSize: 16,
        useNextVariants: true,
        fontFamilyMonospace: '"Roboto Mono", "Menlo", "Monaco", "Courier", monospace'
    },
    spacing: {
        smallest: 1,
        small: 2,
        unit: 4,
        large: 8,
        larger: 16,
        largest: 32,
    },
    overrides: {
        MuiIconButton: {
            root: {
                padding: 6,
            }
        }
    }
});

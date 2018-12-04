module.exports = {
    use: [
        [
            '@neutrinojs/react',
            {
                html: {
                    title: 'CodaLab Worksheets',
                },
                babel: {
                    presets: [
                        '@babel/preset-env',
                        '@babel/preset-react',
                        '@babel/preset-flow',
                    ],
                    plugins: [
                        '@babel/plugin-proposal-class-properties',
                    ]
                }
            }
        ],
        '@neutrinojs/jest',
    ]
};

module.exports = {
  title: 'CodaLab Worksheets',
  tagline: 'Accelerating reproducible computational research.',
  url: 'https://docs.worksheets.codalab.org',
  baseUrl: '/',
  favicon: 'img/favicon.ico',
  organizationName: 'facebook', // Usually your GitHub org/user name.
  projectName: 'co', // Usually your repo name.
  themeConfig: {
    navbar: {
      title: '',
      logo: {
        alt: 'My Site Logo',
        src: 'https://worksheets.codalab.org/img/codalab-logo.png',
      },
      links: [
        {
          to: 'docs/',
          activeBasePath: 'docs',
          label: 'Docs',
          position: 'left',
        },
        {to: 'blog', label: 'Blog', position: 'left'},
        {
          href: 'https://worksheets.codalab.org',
          label: 'Open CodaLab Worksheets',
          position: 'right',
        },        
        {
          href: 'https://github.com/codalab/codalab-worksheets',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Introduction',
              to: 'docs/',
            },
            {
              label: 'CLI Reference',
              to: 'docs/CLI-Reference/',
            },
            {
              label: 'REST API Reference',
              to: 'docs/REST-API-Reference/',
            },
          ],
        },
        {
          title: 'About',
          items: [
            {
              label: 'About Us',
              to: 'docs/About',
            },
            {
              label: 'FAQ',
              to: 'docs/FAQ',
            },
            {
              label: 'Privacy',
              to: 'docs/Privacy',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Blog',
              to: 'blog',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/codalab/codalab-worksheets',
            },
          ],
        },
      ],
      copyright: `Built with Docusaurus.`,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          homePageId: 'Introduction',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl:
            'https://github.com/codalab/codalab-worksheets/edit/master/docs-site/',
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/codalab/codalab-worksheets/edit/master/docs-site/blog/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};

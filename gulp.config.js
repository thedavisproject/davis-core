'use strict';

module.exports = function() {
  var root = './',
    srcRoot = root + 'src/',
    testRoot = root + 'test/',
    config = {
      root: root,
      srcRoot: srcRoot,
      testRoot: testRoot,
      allJs: function(drillPath){
        drillPath = drillPath ? drillPath + '/' : '';
        return [
          srcRoot + drillPath + '**/*.js',
          testRoot + drillPath + '**/*.js',
          root + '*.js'
        ];
      },
      testFiles: function(drillPath){
        drillPath = drillPath ? drillPath + '/' : '';
        return [
          testRoot + drillPath + '**/*.js'
        ];
      },
      packages: [
        root + 'package.json'
      ]
    };

  return config;
};

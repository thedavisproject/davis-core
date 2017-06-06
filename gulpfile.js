var gulp = require('gulp'),
  tasks = require('davis-shared').build,
  config = require('./gulp.config')(),
  argv = require('yargs').argv;

var drillPath = argv.drill,
  allJs = config.allJs(drillPath),
  testFiles = config.testFiles(drillPath);

gulp.task('test', tasks.test(testFiles));

gulp.task('lint', tasks.lint(allJs));

gulp.task('watch', function() {
  gulp.watch(allJs, ['lint', 'test']);
});

gulp.task('bump', tasks.bump(['./package.json'], argv.level || 'patch'));

gulp.task('default', ['watch', 'lint', 'test']);

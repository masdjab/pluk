# - gem spec required attributes:
#   files, name, summary, version
# - gem spec optional attributes:
#   author or authors, description, email, homepage, 
#   license or licenses, metadata

Gem::Specification.new do |s|
  s.name        = 'pluk'
  s.version     = '1.0.0.17'
  s.date        = '2019-05-18'
  s.summary     = 'Simple MySQL ORM'
  s.description = ''
  s.author      = 'Heryudi Praja'
  s.email       = 'mr_orche@yahoo.com'
  s.files       = ['lib/pluk.rb']
  s.homepage    = ''
  s.license     = 'MIT'
  
  # s.add_development_dependency 'rspec', '~> 3.7'
end

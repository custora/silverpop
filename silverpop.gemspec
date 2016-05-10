Gem::Specification.new do |s|
  s.name     = "silverpop"
  s.version  = "1.2.3"
  s.date     = "2013-07-01"
  s.summary  = "Silverpop Engage and Transact API -- Extracted from ShoeDazzle.com, gemified by Billfloat"
  s.email    = "bill@billfloat.com"
  s.homepage = "http://github.com/billfloat/silverpop/tree/master"
  s.description = "Silverpop allows for seamless integration from Ruby with the Engage and Transact API."
  s.authors  = ["George Truong, Bill Abney, Mario Zaizar, Sergey Gopkalo"]

  s.has_rdoc = false
  s.rdoc_options = ["--main", "README.md"]
  s.extra_rdoc_files = ["README.md"]

  s.files         = `git ls-files`.split($\)
  s.executables   = s.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  s.test_files    = s.files.grep(%r{^(test|spec|features)/})
  s.name          = "silverpop"
  s.require_paths = ["lib"]

  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'ftpd'
  s.add_development_dependency 'webmock'
end

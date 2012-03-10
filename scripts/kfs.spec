%define debug_package %{nil}
%define debug_packages %{nil}

Summary: KFS Binary Package
Name: kfs
Version: 0.2.3
Release: 1
License: Apache
Group: System Environment
URL: http://kosmosfs.sourceforge.net
Source0: %{name}-%{version}.tar.gz
#BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRoot: %{_builddir}

%description
This package contains C++ binary distribution of KFS

%prep
%setup -q

%build

%install
#rm -rf $RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%post


%files
%defattr(-,root,root,-)
%doc
/%{name}-%{version}/bin/*
/%{name}-%{version}/lib/*


%changelog
* Thu Nov 27 2008 Sriram Rao <sriram@fedora-vm> - 
- Initial build.

